package marmot.mapreduce;

import java.io.IOException;
import java.util.concurrent.CancellationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.StopWatch;
import utils.async.CancellableWork;
import utils.func.FOption;

import marmot.MarmotCore;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.exec.MarmotExecutionException;
import marmot.io.HdfsPath;
import marmot.io.RecordWritable;
import marmot.io.mapreduce.seqfile.MarmotFileOutputFormat;
import marmot.mapreduce.MultiJobPlanExecution.StageContext;
import marmot.optor.MapReduceableRecordSetLoader;
import marmot.optor.RecordSetConsumer;
import marmot.optor.RecordSetFunction;
import marmot.optor.RecordSetLoader;
import marmot.optor.RecordSetOperator;
import marmot.support.HadoopUtils;
import marmot.support.PlanUtils;
import marmot.support.RecordSetOperatorChain;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class MapReduceStage extends Stage implements CancellableWork {
	private static final Logger s_logger = LoggerFactory.getLogger(MapReduceStage.class);
	
	public static final String PROP_MAP_INPUT_SCHEMA = "marmot.mapreduce.map.input_schema";
	public static final String PROP_MAP_PLAN = "marmot.mapreduce.map.plan";
	public static final String PROP_MAP_OUTPUT_KEY = "marmot.mapreduce.map.output_key";
	public static final String PROP_MAP_OUTPUT_SCHEMA = "marmot.mapreduce.map.output_schema";
	public static final String PROP_MAP_OUTPUT_COMPRESS = "mapreduce.map.output.compress";
	public static final String PROP_MAP_OUTPUT_COMPRESS_CODEC = "mapreduce.map.output.compress.codec";

	public static final String PROP_COMBINE_INPUT_SCHEMA = PROP_MAP_OUTPUT_SCHEMA;
	public static final String PROP_COMBINE_PLAN = "marmot.mapreduce.combine.plan";
	
	public static final String PROP_REDUCE_INPUT_SCHEMA = "marmot.mapreduce.reduce.input_schema";
	public static final String PROP_REDUCE_PLAN = "marmot.mapreduce.reduce.plan";
	
	public static final String PROP_NEXTJOB_PLAN = "marmot.mapreduce.nextjob_plan";

	private final MarmotCore m_marmot;
	final StageContext m_context;
	// empty 이면 명시적으로 map.output.compression을 사용하지 않는 것을 의미함
	// null이면 설정 파일에 따라  map.output.compression 사용 여부가 결정됨.
	private final FOption<String> m_mapOutputCompressCodecName;	 
	
	RecordSetOperatorChain m_mapperChain;
	RecordSetOperatorChain m_combinerChain;
	RecordSetOperatorChain m_reducerChain;
	private FOption<MarmotMapOutputKeyColumns> m_mapOutputKeyColumns = FOption.empty();
	// 'm_mapOutputKeyColumns'가 설정되지 않은 경우에만 의미가 있음.
	@SuppressWarnings("rawtypes")
	private FOption<Class<? extends Partitioner>> m_partitionerClass = FOption.empty();
	int m_reducerCount = 0;
	
	private volatile Job m_job = null;
	private StopWatch m_watch;
	
	private MapReduceableRecordSetLoader m_loader;
	private MapReduceTerminal m_terminal;
	private FOption<HdfsPath> m_outputPath;
	
	MapReduceStage(MarmotCore marmot, StageContext ctx, String mapOutputCompressCodec) {
		m_marmot = marmot;
		m_context = ctx;
		
		switch ( FOption.ofNullable(mapOutputCompressCodec).getOrElse("default").toLowerCase() ) {
			case "default":
				m_mapOutputCompressCodecName = marmot.getDefaultMapOutputCompressCodecName();
				break;
			case "none":
				m_mapOutputCompressCodecName = FOption.empty();
				break;
			default:
				m_mapOutputCompressCodecName = FOption.of(mapOutputCompressCodec);
				break;
		}
		
		whenFinishedAsync(result -> {
			result.ifSuccessful(r -> {
						s_logger.info("finished the stage: {}:{} (elapsed={})",
								m_context.m_stageId, toShortString(), m_watch.stopAndGetElpasedTimeString());
					})
					.ifFailed(cause -> {
						s_logger.info("cancelled the stage: {}:{} cause={}",
										m_context.m_stageId, toShortString(), cause);
					})
					.ifNone(() -> {
						s_logger.info("cancelled the stage: {}:{} (elapsed={})",
								m_context.m_stageId, toShortString(), m_watch.stopAndGetElpasedTimeString());
					});
		});
	}

	@Override
	public String getId() {
		return m_context.m_stageId;
	}
	
	public RecordSchema getInputRecordSchema() {
		return m_mapperChain.getInputRecordSchema();
	}

	@Override
	public RecordSchema getOutputRecordSchema() {
		return getTailChain().getOutputRecordSchema();
	}
	
	public RecordSchema getReduceInputRecordSchema() {
		return m_reducerChain.getInputRecordSchema();
	}
	
	void setMapOutputKey(MarmotMapOutputKeyColumns keyCols) {
		m_mapOutputKeyColumns = FOption.of(keyCols);
	}
	
	void setRecordSetLoader(MapReduceableRecordSetLoader loader) {
		if ( m_loader != null ) {
			throw new MarmotExecutionException("multiple MapReduceableRecordSetLoader");
		}
		
		m_loader = loader;
	}
	
	@SuppressWarnings("rawtypes")
	void setPartitionerClass(Class<? extends Partitioner> cls) {
		m_partitionerClass = FOption.of(cls);
	}

	@Override
	public FOption<HdfsPath> getOutputPath() {
		return m_outputPath;
	}

	@Override
	public Path getWorkspace() {
		return m_context.m_stageWorkspace;
	}
	
	@Override
	protected Void executeWork() throws CancellationException, Exception {
		m_watch = StopWatch.start();
		
		m_job = buildMapReduceJob();
		checkCancelled();
		
		s_logger.info("starting a MR stage: {}:{}", m_context.m_stageId, toShortString());
		boolean done = m_job.waitForCompletion(true);
		if ( !done ) {
			checkCancelled();
			throw new MarmotExecutionException("MR execution failed: stage=" + this);
		}
		
		return null;
	}
	
	@Override
	public boolean cancelWork() {
		if ( m_job != null ) {
			try {
				m_job.killJob();
				return true;
			}
			catch ( IOException e ) {
				return false;
			}
		}
		else {
			return true;
		}
	}
	
	public void add(RecordSetOperator optor) {
		getTailChain().add(optor);
		if ( optor instanceof MapReduceTerminal ) {
			m_terminal = (MapReduceTerminal)optor;
		}
	}
	
	public RecordSetOperator getLastOperator() {
		return getTailChain().getLast();
	}
	
	public RecordSetOperatorChain getTailChain() {
		if ( m_reducerChain != null && m_reducerChain.length() > 0 ) {
			return m_reducerChain;
		}
		if ( m_combinerChain != null && m_combinerChain.length() > 0 ) {
			return m_combinerChain;
		}
		
		return m_mapperChain;
	}
	
	public String toShortString() {
		String pathList = m_loader.getInputString();
		return String.format("in=%s, out=%s", toShortMessage(pathList, 50),
							getOutputPath().map(Object::toString).getOrElse("none"));
	}
	
	public void notifyStageWorkspaceAware() {
		m_mapperChain.getAll().stream()
					.filter(optor->optor instanceof StageWorkspaceAware)
					.map(optor -> (StageWorkspaceAware)optor)
					.forEach(optor -> optor.setStageWorkspace(m_context.m_stageWorkspace));
		m_combinerChain.getAll().stream()
					.filter(optor->optor instanceof StageWorkspaceAware)
					.map(optor -> (StageWorkspaceAware)optor)
					.forEach(optor -> optor.setStageWorkspace(m_context.m_stageWorkspace));
		m_reducerChain.getAll().stream()
					.filter(optor->optor instanceof StageWorkspaceAware)
					.map(optor -> (StageWorkspaceAware)optor)
					.forEach(optor -> optor.setStageWorkspace(m_context.m_stageWorkspace));
	}
	
	@Override
	public String toString() {
		String pathList = "unknown";
		if ( m_mapperChain != null && m_mapperChain.length() > 0 ) {
			RecordSetLoader loader = m_mapperChain.getRecordSetLoader();
			if ( loader instanceof MapReduceableRecordSetLoader ) {
				pathList = ((MapReduceableRecordSetLoader)loader).getInputString();
			}
		}

		String linesep = String.format("%n");
		StringBuilder builder = new StringBuilder();
		
		String head = String.format("%s: in=%s, out=%s", m_context.m_stageId,
									toShortMessage(pathList, 50), getOutputPath());
		builder.append(head);
		
		if ( m_mapperChain != null && m_mapperChain.length() > 0 ) {
			String str= m_mapperChain.streamOperators()
										.map(RecordSetOperator::toString)
										.join(", ", "{", "}");
			builder.append(linesep).append("mappers=").append(str);
		}
		if ( m_combinerChain != null && m_combinerChain.length() > 0 ) {
			String str= m_combinerChain.streamOperators()
										.map(RecordSetOperator::toString)
										.join(", ", "{", "}");
			builder.append(linesep).append("combiners=").append(str);
		}
		if ( m_reducerChain != null && m_reducerChain.length() > 0 ) {
			String str= m_reducerChain.streamOperators()
										.map(RecordSetOperator::toString)
										.join(", ", "{", "}");
			builder.append(linesep).append("reducers=").append(str);
		}
		
		return builder.toString();
	}
	
	static RecordSchema getMapperInputSchema(Configuration conf) {
		String schemaDesc = conf.get(PROP_MAP_INPUT_SCHEMA);
		return RecordSchema.parse(schemaDesc);
	}
	
	static void setMapperInputSchema(Configuration conf, RecordSchema schema) {
		conf.set(PROP_MAP_INPUT_SCHEMA, schema.toString());
	}
	
	static Plan getMapperPlan(Configuration conf) {
		String planDesc = conf.get(PROP_MAP_PLAN);
		try {
			return Plan.fromEncodedString(planDesc);
		}
		catch ( IOException e ) {
			throw new MarmotExecutionException("" + e);
		}
	}
	
	static FOption<MarmotMapOutputKeyColumns> getMapOutputKeyColumns(Configuration conf) {
		return FOption.ofNullable(conf.get(PROP_MAP_OUTPUT_KEY))
					.map(MarmotMapOutputKeyColumns::fromString);
	}
	
	static RecordSchema getMapOutputRecordSchema(Configuration conf) {
		String schemaDesc = conf.get(PROP_MAP_OUTPUT_SCHEMA);
		return RecordSchema.parse(schemaDesc);
	}
	
	static RecordSchema getCombinerInputRecordSchema(Configuration conf) {
		return getMapOutputRecordSchema(conf);
	}
	
	static Plan getCombinerPlan(Configuration conf) {
		String planDesc = conf.get(PROP_COMBINE_PLAN);
		try {
			return Plan.fromEncodedString(planDesc);
		}
		catch ( IOException e ) {
			throw new MarmotExecutionException("" + e);
		}
	}
	
	static RecordSchema getReducerInputRecordSchema(Configuration conf) {
		String schemaDesc = conf.get(PROP_REDUCE_INPUT_SCHEMA);
		return RecordSchema.parse(schemaDesc);
	}
	
	static Plan getReducerPlan(Configuration conf) {
		String planDesc = conf.get(PROP_REDUCE_PLAN);
		try {
			return Plan.fromEncodedString(planDesc);
		}
		catch ( IOException e ) {
			throw new MarmotExecutionException("" + e);
		}
	}
	
	static Plan getNextJobPlan(Configuration conf) {
		try {
			return Plan.fromEncodedString(conf.get(PROP_NEXTJOB_PLAN));
		}
		catch ( IOException e ) {
			throw new MarmotExecutionException("" + e);
		}
	}

	private Job buildMapReduceJob() {
		Job job;
		try {
			job = Job.getInstance(m_marmot.getHadoopConfiguration(), m_context.m_stageId);
			job.setJarByClass(getClass());
		}
		catch ( IOException e ) {
			throw new MarmotExecutionException(e);
		}
		
		Configuration jobConf = job.getConfiguration();
		jobConf.set("marmot.stage.workspace", m_context.m_stageWorkspace.toString());

		//
		// initialize Job object
		// (these may be overridden by RecordSetOeprators)
		//
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(RecordWritable.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(RecordWritable.class);
		job.setOutputFormatClass(LazyOutputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, MarmotFileOutputFormat.class);
		
		m_mapperChain.initialize();
		m_combinerChain.initialize();
		m_reducerChain.initialize();
		
		// RecorSetLoader에 따른 설정 작업을 수행한다.
		// 일반적으로 다음의 설정 작업을 수행한다.
		//	- 입력 파일 포맷 설정: job.setInputFormatClass(...)
		// 	- MapReduce 입력 파일 경로 설정: FileInputFormat.addInputPath(job, ...)
		// 	- FileSplit 크기 설정
		m_loader.configure(job);
		
		// configure the mapper
		configureMapper(job);

		// configure the combiner
		configureCombiner(job);
		
		// configure the reducer
		configureReducer(job);
		
		MapReduceJobConfigurer.configure(m_terminal, job);
		if ( m_terminal != null && m_terminal instanceof HdfsWriterTerminal ) {
			m_outputPath = FOption.ofNullable(((HdfsWriterTerminal)m_terminal).getOutputPath())
								.map(p -> HdfsPath.of(m_marmot.getHadoopFileSystem(), p));
		}
		else {
			m_outputPath = FOption.empty();
		}
		
		m_mapOutputCompressCodecName.ifPresent(codecName -> {
			String codecCls = HadoopUtils.getCompressionCodecClassByName(codecName);

			jobConf.set(PROP_MAP_OUTPUT_COMPRESS, "true");
			jobConf.set(PROP_MAP_OUTPUT_COMPRESS_CODEC, codecCls);
			s_logger.info("use map_output_compress: class=" + codecCls);
		})
		.ifAbsent(() -> {
			jobConf.set(PROP_MAP_OUTPUT_COMPRESS, "false");
			s_logger.info("use no map_output_compress");
		});
		
		return job;
	}
	
	private void configureMapper(Job job) {
		job.setMapperClass(MarmotMRMapper.class);
		
		// Map output에 key가 설정된 경우 처리
		m_mapOutputKeyColumns.ifAbsent(() -> {
			job.setMapOutputKeyClass(NullWritable.class);
			m_partitionerClass.ifPresent(job::setPartitionerClass);
		})
		.ifPresent(okc -> okc.configure(job));
		
		boolean mapOnly = m_reducerChain.length() == 0 && m_combinerChain.length() == 0;
		
		RecordSetOperator last = m_mapperChain.getLast();
		if ( last instanceof MapReduceTerminal ) {
			m_terminal = (MapReduceTerminal)last;
		}
		if ( mapOnly && !(last instanceof RecordSetConsumer) ) {
			// 본 stage가 전체 Plan의 마지막 stage가 아닌 상태에서 map-chain의 마지막 연산자가
			// RecordSetConsumer가 아닌 경우는 강제로 IntermediateTerminal을 추가한다.
			IntermediateTerminal terminal = createIntermediateTerminal();
			terminal.initialize(m_marmot, m_mapperChain.getOutputRecordSchema());
			m_terminal = terminal;
			last = terminal;
			m_mapperChain.add(last);
		}
		
		// m_mapperChain에 연산자가 하나밖에 없는 경우에는 첫번째 연산인 loader와 last가 동일하게
		// 되고, configure 호출시 이미 처리되기 때문에 이 경우는 last configure를 제외시킨다.
		if ( m_loader != last ) {
			MapReduceJobConfigurer.configure(last, job);
//			m_mapperChain.fstream().forEach(optor -> MapReduceJobConfigurer.configure(optor, job));
		}
		
		// map 단계에서 RecordSetLoader는 실제로 필요없기 때문에 제거하고, 대신 레코드세트 함수가
		// 필요한 경우 앞에 추가한다.
		RecordSetOperator first = m_mapperChain.removeFirst();
		m_mapperChain.setInputRecordSchema(first.getRecordSchema());
		FOption<RecordSetFunction> func = m_loader.getMapperRecordSetFunction();
		func.ifPresent(f -> m_mapperChain.add(0, f));
		
		String mapperPlanStr = PlanUtils.toPlan("map_plan", m_mapperChain.getAll())
										.toEncodedString();

		Configuration jobConf = job.getConfiguration();
		MapReduceStage.setMapperInputSchema(jobConf, m_loader.getRecordSchema());
		jobConf.set(PROP_MAP_PLAN, mapperPlanStr);
		jobConf.set(PROP_MAP_OUTPUT_SCHEMA, m_mapperChain.getOutputRecordSchema().toString());
	}
	
	private void configureCombiner(Job job) {
		if ( m_combinerChain.length() > 0 ) {
			job.setCombinerClass(MarmotMRCombiner.class);

			String combinePlanStr = m_combinerChain.toPlan("combine_plan")
													.toEncodedString();
			
			Configuration jobConf = job.getConfiguration();
			jobConf.set(PROP_COMBINE_PLAN, combinePlanStr);
			jobConf.set(PROP_COMBINE_INPUT_SCHEMA, m_combinerChain.getOutputRecordSchema().toString());
		}
	}
	
	private void configureReducer(Job job) {
		if ( m_reducerChain.length() > 0 ) {
			job.setReducerClass(MarmotMRReducer.class);
			
			RecordSetOperator last = m_reducerChain.getLast();
			if ( last instanceof MapReduceTerminal ) {
				m_terminal = (MapReduceTerminal)last;
			}
			if ( !(last instanceof RecordSetConsumer) ) {
				IntermediateTerminal terminal = createIntermediateTerminal();
				terminal.initialize(m_marmot, m_reducerChain.getOutputRecordSchema());
				m_reducerChain.add(terminal);
				m_terminal = terminal;
			}
			m_reducerChain.streamOperators()
							.castSafely(MapReduceJobConfigurer.class)
							.forEach(optor -> optor.configure(job));
			
			m_reducerCount = Math.max(1, m_reducerCount);
			job.setNumReduceTasks(m_reducerCount);

			String reducePlanStr = m_reducerChain.toPlan("reduce_plan")
													.toEncodedString();
			
			Configuration jobConf = job.getConfiguration();
			jobConf.set(PROP_REDUCE_PLAN, reducePlanStr);
			jobConf.set(PROP_REDUCE_INPUT_SCHEMA, m_reducerChain.getInputRecordSchema().toString());
		}
		else {
			job.setNumReduceTasks(0);
		}
	}
	
	private IntermediateTerminal createIntermediateTerminal() {
		Path outputPath = new Path(m_context.m_stageWorkspace, "output");
		return new IntermediateTerminal(outputPath);
	}
	
	private String toShortMessage(String msg, int maxLen) {
		if ( msg.length() > maxLen ) {
			msg = msg.substring(0, maxLen) + "...";
		}
		return msg;
	}
}