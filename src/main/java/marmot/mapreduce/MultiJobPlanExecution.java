package marmot.mapreduce;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;

import javax.annotation.concurrent.GuardedBy;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import utils.async.CancellableWork;
import utils.async.Guard;
import utils.func.FOption;
import utils.func.Lazy;
import utils.func.Try;
import utils.stream.FStream;

import marmot.MarmotCore;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.RecordSetNotExistsException;
import marmot.exec.MarmotExecutionException;
import marmot.exec.PlanExecution;
import marmot.io.HdfsPath;
import marmot.optor.LoadMarmotFile;
import marmot.optor.MapReduceJoint;
import marmot.optor.MapReduceableRecordSetLoader;
import marmot.optor.NonParallelizable;
import marmot.optor.RecordSetConsumer;
import marmot.optor.RecordSetFunction;
import marmot.optor.RecordSetLoader;
import marmot.optor.RecordSetOperator;
import marmot.optor.StoreAsHeapfile;
import marmot.optor.support.LoadEmptyMarmotFile;
import marmot.optor.support.RecordSetOperators;
import marmot.support.RecordSetOperatorChain;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MultiJobPlanExecution extends PlanExecution implements CancellableWork {
	private static final Logger s_logger = LoggerFactory.getLogger(MultiJobPlanExecution.class);
	static final Path TEMP_DIR_PREFIX = new Path("tmp/marmot/mapreduce");
	
	private final MarmotCore m_marmot;
	private boolean m_disableLocalExecution;
	private String m_mapOutputCompressCodec = "default";
	final String m_name;
	private final String m_id;
	
	private FOption<HdfsPath> m_outputPath = FOption.empty();

//	private Plan m_plan;
	private RecordSetOperatorChain m_chain;
	private OperatorActivations m_activations;
	private int m_nextOpIndex;
	private List<Stage> m_stages = Lists.newArrayList();
	private final Lazy<RecordSchema> m_schema;
	
	private final Guard m_guard = Guard.create();
	@GuardedBy("m_guard") private Stage m_currentStage = null;
	
	public static MultiJobPlanExecution create(MarmotCore marmot, Plan plan) {
		return new MultiJobPlanExecution(marmot, plan.getName(),
										RecordSetOperatorChain.from(marmot, plan));
	}
	
	public static PlanExecution create(MarmotCore marmot, String jobName,
										RecordSetOperatorChain chain) {
		return new MultiJobPlanExecution(marmot, jobName, chain);
	}
	
	MultiJobPlanExecution(MarmotCore marmot, String jobName, RecordSetOperatorChain chain) {
		m_marmot = marmot;
		
		m_name = jobName;
		m_id = UUID.randomUUID().toString();
		
		m_chain = chain;
		if ( chain.length() == 0 ) {
			throw new MarmotExecutionException("no operators in the operator chain");
		}

		// MultiJobPlanExecution는 plan execution 용도 뿐만 아니라,
		// local execution 여부 확인용으로도 객체를 생성하기 때문에
		// 아래 코드는 실제 execute하는 메소드로 옮긴다.
//		RecordSetOperator last = chain.getLast();
//		if ( !(last instanceof RecordSetConsumer) ) {
//			throw new MarmotExecutionException("last operator is not a RecordSetConsumer: plan=" + m_plan);
//		}
		
		m_activations = OperatorActivations.from(chain);
		m_nextOpIndex = 0;

		m_schema = Lazy.of(() -> m_chain.getOutputRecordSchema());
	}
	
	public String getName() {
		return m_name;
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema.get();
	}

	@Override
	public void setDisableLocalExecution(boolean flag) {
		m_disableLocalExecution = flag;
	}

	@Override
	public void setMapOutputCompressCodec(String codec) {
		m_mapOutputCompressCodec = codec;
	}
	
	public boolean isLocallyExecutable() {
		StageContext context = new StageContext();
		context.m_stageId = String.format("%s[%d]", m_name, 0);
		context.m_stageWorkspace = getStageWorkspace(0);
		
		try {
			int endIdx = findLocallyExecutableOperators(context, 0, null);
			return endIdx+1 >= m_activations.length();
		}
		catch ( IOException e ) {
			throw new MarmotExecutionException("" + e);
		}
	}

	@Override
	protected Void executeWork() throws CancellationException, Exception {
		RecordSetOperator last = m_chain.getLast();
		if ( !(last instanceof RecordSetConsumer) ) {
			throw new MarmotExecutionException("last operator is not a RecordSetConsumer: chain=" + m_chain);
		}
		
		try {
			m_marmot.setProperty("marmot.workspace.dir", getWorkspace().toString());
			
			FileSystem fs = m_marmot.getHadoopFileSystem();
			if ( !fs.exists(getWorkspace()) ) {
				mkdir(getWorkspace());
			}
		}
		catch ( Throwable e ) {
			throw new MarmotExecutionException("fails to setup MultiJobPlanExecution", e);
		}

		try {
			while ( true ) {
				Stage stage = locateNextStage();
				if ( stage == null ) {
					// 모든 stage 종료
					break;
				}
				
				m_guard.lock();
				try {
					// 외부에서 중단을 요청했는지 확인함.
					if ( !isRunning() ) {
						return null;
					}
					
					m_currentStage = stage;
					m_guard.signalAll();
				}
				finally {
					m_guard.unlock();
				}

				// 다음 stage를 수행함
				m_currentStage.run();
				
				// 결과 반영
				m_outputPath = m_currentStage.getOutputPath();
				m_stages.add(m_currentStage);
				
				s_logger.debug("finished: stage[{}]", m_currentStage.getId());
			}
			
			s_logger.info("finished: plan[{}]", m_name);
			return null;
		}
		catch ( RecordSetNotExistsException e ) {
			s_logger.warn("PlanExecution is stopped due to empty input RecordSet: cause="
						+ e.getMessage());
			throw e;
		}
		catch ( InterruptedException e ) {
			throw new CancellationException("thread was interrupted");
		}
		finally {
			try {
				relocateResultFile();
			}
			catch ( IOException e ) {
				s_logger.error("fails to relocate the final result", e);
			}
			
			Try.run(this::cleanUpTempFiles);
		}
	}
	
	@Override
	public boolean cancelWork() {
		try {
			return m_guard.awaitCondition(() -> m_currentStage != null || !isRunning())
							.andGet(() -> {
								if ( m_currentStage != null ) {
									return m_currentStage.cancel(true);
								}
								else {
									return true;
								}
							});
		}
		catch ( InterruptedException e ) {
			return false;
		}
	}
	
	private Stage locateNextStage() throws IOException {
		RecordSchema inputSchema = FStream.from(m_stages)
											.findLast()
											.map(Stage::getOutputRecordSchema)
											.getOrNull();
		
		if ( m_nextOpIndex < m_activations.length() ) {
			StageContext context = new StageContext();
			
			int stageNo = m_stages.size();
			context.m_stageId = String.format("%s[%d]", m_name, stageNo);
			context.m_stageWorkspace = getStageWorkspace(stageNo);
			
			int endIdx = findLocallyExecutableOperators(context, m_nextOpIndex, inputSchema);
			if ( endIdx > m_nextOpIndex ) {
				// 지역적으로 수행 가능한 경우.
				return locateNextLocalStage(context, endIdx);
			}
			else {
				return locateNextMapReduceStage(context);
			}
		}
		else {
			return null;
		}
	}
	
	@Override
	public String toString() {
		String stageStr = String.format("%d(%d/%d)", m_stages.size(), m_nextOpIndex,
																		m_activations.length());
		if ( m_stages.size() > 0 ) {
			Stage stage = Iterables.getLast(m_stages);
			stageStr = stageStr + ",stage=" + stage;
		}
		
		return String.format("MultiJobExecution[%s]", stageStr);
	}
	
	static class StageContext {
		String m_stageId;
		Path m_stageWorkspace;
		
		public String toString() {
			return String.format("%s: workspace=%s", m_stageId, m_stageWorkspace);
		}
	}

	private int findLocallyExecutableOperators(StageContext ctx, int startIdx,
												RecordSchema inputSchema) throws IOException {
		RecordSetOperator optor = m_activations.getLeafOperatorAt(startIdx, inputSchema, ctx);
		if ( !(optor instanceof RecordSetLoader) ) {
			// 새로운 phase가 RecordSetLoader로 시작하지 않는 경우.
			//
			if ( m_outputPath.isAbsent() ) {
				// RecordSetLoader로 새 phase가 시작되지 않은 경우는 오류로 간주한다.
				throw new MarmotExecutionException("the first operator must be a "
												+ "RecordSetLoader: first=" + optor);
			}
			
			// 이전 phase의 수행으로 임시 결과가 생성된 경우.
			// 이전 결과를 사용하기 위해 강제로 LoadMarmotFile 연산을 추가시키고 이 연산부터
			// 본 phase를 시작하도록 한다.
			LoadMarmotFile load = LoadMarmotFile.from(m_outputPath.get().toString());
			load.initialize(m_marmot);
			m_activations.add(startIdx, optor = load);
		}
		else if ( optor instanceof LoadEmptyMarmotFile ) {
			// 입력 파일이 존재하지 않는 경우는 LoadEmptyMarmotFile가 반환되는데,
			// 이때는 전체를 local하게 수행하도록 해서 바로 종료되도록 한다.
			return m_activations.length()-1;
		}
		else if ( !(optor instanceof MapReduceableRecordSetLoader) ) {
			// RecordSetLoader 연산이 본 phase의 첫번째 연산이지만,
			// MapReduce를 지원하지 않는 경우, 이후 연산들은 Map/Reduce 작업으로 수행될 수
			// 없기 때문에, 이 연산부터 'RecordSetConsumer' 연산까지를 하나의 phase로 보고,
			// 이 전체를 local하게 수행하도록 한다.
			//
			for ( int idx = startIdx+1; idx < m_activations.length(); ++idx ) {
				inputSchema = RecordSetOperators.getOutputRecordSchema(optor); 
				optor = m_activations.getLeafOperatorAt(idx, inputSchema, ctx);
				
				MapReduceJoint joint = optor.getMapReduceJoint(inputSchema);
				if ( joint != null ) {
					throw new MarmotExecutionException("operator cannot be run in local mode: optor="
													+ optor);
				}
				
				if ( optor instanceof RecordSetConsumer ) {
					return idx;
				}
				inputSchema = optor.getRecordSchema();
			}
			throw new MarmotExecutionException("the last operator must be a "
											+ "RecordSetCollector: last=" + optor);
		}
		
		MapReduceableRecordSetLoader rsetLoader = (MapReduceableRecordSetLoader)optor;
		PlanMRExecutionMode mode = rsetLoader.getExecutionMode(m_marmot);
		if ( mode == PlanMRExecutionMode.NON_LOCALLY_EXECUTABLE ) {
			// 비록 입력 파일의 크기가 커서 local하게 수행되기 어려운 경우는
			// 이번 phase는 MapReduce 방식으로 처리되도록 한다.
			// 그러나, 만일  바로 다음 연산이 NonParallelizable인 경우는 지역적으로
			// 수행될 수 밖에 없으므로, local하게 처리되도록 한다.
			inputSchema = rsetLoader.getRecordSchema();
			optor = m_activations.getLeafOperatorAt(startIdx+1, inputSchema, ctx);
			if ( optor == null || !(optor instanceof NonParallelizable) ) {
				return -1;
			}
		}
		
		// 입력 파일의 크기에 따라 실행 방식이 'LOCALLY_EXECUTABLE'
		// 또는 'CONDITIONALLY_EXECUTABLE'이거나,
		// 비록 실행 방식이 'NON_LOCALLY_EXECUTABLE' 이더라도 바로 다음 연산이
		// 'NonParallelizable'이어서 지역적으로 수행되어야 하는 경우.
		// 어느 연산까지 local하게 수행할 수 있는지를 계산한다.
		
		int lastNonParallelIdx = -1;
		for ( int idx = startIdx+1; idx < m_activations.length(); ++idx ) {
			inputSchema = RecordSetOperators.getOutputRecordSchema(optor);
			optor = m_activations.getLeafOperatorAt(idx, inputSchema, ctx);
			
			if ( optor instanceof RecordSetConsumer ) {
				if ( optor instanceof NonParallelizable || !m_disableLocalExecution ) {
					return idx;
				}
				else {
					return lastNonParallelIdx;
				}
			}
			
			RecordSetFunction func = (RecordSetFunction)optor;
			if ( func.isMapReduceRequired() ) {
				// 수행할 연산 중에 'MapReduceRequired'한 연산이 있는 경우는
				// 다른 앞선 연산을 local하게 수행해도 별로 소득이 없기 때문에
				// 전체를 map-reduce로 수행되도록 한다.
				return -1;
			}
			
			if ( optor instanceof NonParallelizable ) {
				lastNonParallelIdx = idx;
				continue;
			}
			
			if ( m_disableLocalExecution ) {
				return lastNonParallelIdx;
			}
			
			if ( mode == PlanMRExecutionMode.CONDITIONALLY_EXECUTABLE ) {
				// loader에 의해 conditionally executable한 상태로 파판정되면
				// 남은 연산자들 중에서 하나라도 'isStreamable()'가 없는 경우만 local하게 수행시키고,
				// 그렇지 않은 경우는, 일부는 local로 나머지 일부는 MR로 수행하기 보다는
				// 그냥 전체를 MR를 수행시킨다.
				//
				if ( !func.isStreamable() ) {
					return lastNonParallelIdx;
				}
			}
		}
		
		return m_activations.length() - 1;
	}
	
	private LocalStage locateNextLocalStage(StageContext ctx, int endIdx) {
		// 수행될 지역 stage에 포함될 연사자들을 구한다.
		List<RecordSetOperator> optors = m_activations.subList(m_nextOpIndex, endIdx+1);
		LocalStage stage = new LocalStage(m_marmot, ctx, optors);
		m_nextOpIndex = endIdx + 1;
		
		return stage;
	}
	
	private MapReduceStage locateNextMapReduceStage(StageContext ctx) {
		MapReduceStage stage = new MapReduceStage(m_marmot, ctx, m_mapOutputCompressCodec);
		
		// 남은 연산자들을 순서대로 검토하여 이번 Map-Reduce 작업에서 수행시킬 수 있는
		// 연산자들을 찾아 stage의 stage에 삽입시킨다.
		addOperatorsIntoStage(stage);
		
		if ( stage.m_combinerChain == null ) {
			stage.m_combinerChain = RecordSetOperatorChain.from(m_marmot,
															stage.m_mapperChain.getOutputRecordSchema());
		}
		if ( stage.m_reducerChain == null ) {
			stage.m_reducerChain = RecordSetOperatorChain.from(m_marmot,
															stage.m_combinerChain.getOutputRecordSchema());
		}

		RecordSetOperator last = stage.getLastOperator();
		if ( !(last instanceof RecordSetConsumer) ) {
			// 선정된 MR job의 마지막 연산이 'MapReduceTerminal'가 아닌 경우는
			// 마지막 연산 수행 결과를 임시 파일에 기록하여, 다음 MR 단계에서 수행될 연산 앞에
			// 임시 파일을 읽는 연산을 추가하여 계속 연결되도록 한다.
			//
			Path outputPath = new Path(stage.getWorkspace(), "output");
			
			// 연산 결과를 임시 파일에 저장하는 연산자를 생성하여 추가 
			RecordSetConsumer store = new StoreAsHeapfile(outputPath);
			stage.add(store);
			m_activations.add(m_nextOpIndex++, store);
			
			// 다음 MR 단계에서 생성된 임시파일을 읽는 연산 추가
			RecordSetLoader load = LoadMarmotFile.from(outputPath);
			m_activations.add(m_nextOpIndex, load);
		}
		stage.notifyStageWorkspaceAware();
		
		return stage;
	}
	
	private void addOperatorsIntoStage(MapReduceStage stage) {
		StageContext ctx = stage.m_context;
		
		RecordSetOperatorChain chain = stage.m_mapperChain
									= RecordSetOperatorChain.from(m_marmot);
		while ( m_nextOpIndex < m_activations.length() ) {
			final RecordSchema inputSchema = chain.getOutputRecordSchema();
			final RecordSetOperator optor
							= m_activations.getLeafOperatorAt(m_nextOpIndex++, inputSchema, ctx);
			
			if ( optor instanceof MapReduceableRecordSetLoader ) {
				stage.setRecordSetLoader((MapReduceableRecordSetLoader)optor);
			}
			else if ( chain.length() == 0 ) {
				throw new MarmotExecutionException("The first operator should be "
									+ "a MapReduceableRecordSetLoader: first=" + optor);
			}
			
			if ( optor instanceof MapReduceTerminal ) {
				chain.add(optor);
				break;
			}
			else if ( optor instanceof NonParallelizable ) {
				--m_nextOpIndex;
				break;
			}

			MapReduceJoint joint = optor.getMapReduceJoint(inputSchema);
			if ( joint != null ) {
				// map단계가 아닌 단계(reduce 단계)에서는 MapReduceJoint 연산을
				// 처리할 수 없기 때문에 이 연산 전까지의 연산들을 하나의 Map/Reduce 작업으로
				// 형성하여 반환한다.
				if ( chain != stage.m_mapperChain ) {
					--m_nextOpIndex;
					break;
				}

				stage.m_mapperChain.addAll(joint.getMapperAll());
				stage.m_combinerChain = RecordSetOperatorChain
											.from(m_marmot, stage.m_mapperChain.getOutputRecordSchema())
											.addAll(joint.getCombinerAll());
				stage.m_reducerChain = RecordSetOperatorChain
											.from(m_marmot, stage.m_combinerChain.getOutputRecordSchema())
											.addAll(joint.getReducerAll());
				joint.getMapOutputKey().ifPresent(stage::setMapOutputKey);
				joint.getPartitionerClass().ifPresent(stage::setPartitionerClass);
				stage.m_reducerCount = joint.getReducerCount();
				
				chain = stage.m_reducerChain;
			}
			else {
				chain.add(optor);
			}
		}
	}
	
	private Path getWorkspace() {
		return new Path(TEMP_DIR_PREFIX, m_name+"-"+m_id);
	}
	
	private Path getStageWorkspace(int stageIdx) {
		return new Path(getWorkspace(), "step_"+stageIdx);
	}
	
	private void mkdir(Path path) throws IOException {
		FileSystem fs = m_marmot.getHadoopFileSystem();
		
		if ( fs.exists(path) ) {
			return;
		}
		mkdir(path.getParent());
		fs.mkdirs(path);
	}
	
	private void relocateResultFile() throws IOException {
		FileSystem fs = m_marmot.getHadoopFileSystem();
		
		HdfsPath outputPath = m_outputPath.getOrNull();
		if ( outputPath == null || !outputPath.exists() || outputPath.isFile() ) {
			return;
		}
		
		// remove the file '_SUCESS'
		outputPath.child("_SUCCESS").delete();
		List<HdfsPath> resultFileList = outputPath.walkTree(false).toList();
		if ( resultFileList.size() > 1 ) {
			// rename the files starts with 'part-' to the files starts with 'outputPath'
//			resultFileList.stream()
//						.forEach(path -> {
//							try {
//								String fname = path.getName();
//								if ( fname.startsWith("part-") ) {
//									int idx = fname.lastIndexOf('-');
//									fname = fname.substring(idx+1);
//									
//								}
//								path.moveTo(outputPath.child(fname));
//							}
//							catch ( Exception ignored ) {
//								s_logger.error("fails to rename output file: path=" + path);
//							}
//						});
		}
		else if ( resultFileList.size() == 1 ) {
			HdfsPath tempFinalPath = HdfsPath.of(fs, new Path(getWorkspace(), "tmp_result"));
			resultFileList.get(0).moveTo(tempFinalPath);
			outputPath.delete();
			tempFinalPath.moveTo(outputPath);
		}
	}
	
	private void cleanUpTempFiles() throws IOException {
		FileSystem fs = m_marmot.getHadoopFileSystem();
		fs.delete(getWorkspace(), true);
	}
}
