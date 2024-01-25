package marmot;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.dataset.Catalogs;
import marmot.dataset.DataSet;
import marmot.dataset.DataSetExistsException;
import marmot.dataset.DataSetImpl;
import marmot.dataset.DataSetType;
import marmot.exec.AnalysisNotFoundException;
import marmot.exec.AsyncMarmotExecution;
import marmot.exec.CompositeAnalysis;
import marmot.exec.ExecutionNotFoundException;
import marmot.exec.MarmotAnalysis;
import marmot.exec.MarmotAnalysisManager;
import marmot.exec.MarmotExecution;
import marmot.exec.MarmotExecutionException;
import marmot.exec.MarmotExecutionRegistry;
import marmot.exec.PlanExecution;
import marmot.io.MarmotFileNotFoundException;
import marmot.module.MarmotModuleRegistry;
import marmot.optor.CreateDataSetOptions;
import marmot.optor.RecordSetConsumer;
import marmot.optor.RecordSetOperator;
import marmot.optor.StoreDataSet;
import marmot.optor.StoreDataSetOptions;
import marmot.support.RecordSetOperatorChain;
import utils.Throwables;
import utils.UnitUtils;
import utils.Utilities;
import utils.func.FOption;
import utils.func.Try;
import utils.func.Tuple;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotServer implements MarmotRuntime {
	private final static Logger s_logger = LoggerFactory.getLogger(MarmotServer.class);
	private static final long DEF_GRPC_MSG_SIZE = UnitUtils.parseByteSize("8m");

	private final MarmotCore m_marmot;
	private final ScheduledExecutorService m_executor;
	private final MarmotExecutionRegistry m_execRegistry;
	private final MarmotAnalysisManager m_analysisMgr;
	
	public MarmotServer(MarmotCore marmot) {
		m_marmot = marmot;
		m_executor = Executors.newScheduledThreadPool(16);
		m_execRegistry = new MarmotExecutionRegistry(m_executor);
		m_analysisMgr = new MarmotAnalysisManager(m_marmot, m_execRegistry, m_executor);
	}

	public void shutdown() {
		m_execRegistry.shutdown();
		m_executor.shutdown();
	}
	
	public MarmotCore getMarmotCore() {
		return m_marmot;
	}
	
	

	@Override
	public DataSetImpl getDataSet(String id) {
		return m_marmot.getDataSet(id);
	}

	@Override
	public DataSetImpl getDataSetOrNull(String id) {
		return m_marmot.getDataSetOrNull(id);
	}

	@Override
	public List<DataSet> getDataSetAll() {
		return m_marmot.getDataSetAll();
	}

	@Override
	public boolean deleteDataSet(String id) {
		return m_marmot.deleteDataSet(id);
	}

	@Override
	public void moveDataSet(String id, String newId) {
		m_marmot.moveDataSet(id, newId);
	}

	@Override
	public List<DataSet> getDataSetAllInDir(String folder, boolean recursive) {
		return m_marmot.getDataSetAllInDir(folder, recursive);
	}

	@Override
	public DataSetImpl createDataSet(String dsId, RecordSchema schema, CreateDataSetOptions opts)
		throws DataSetExistsException {
		return m_marmot.createDataSet(dsId, schema, opts);
	}

	public DataSetImpl createDataSet(String dsId, Plan plan, StoreDataSetOptions opts)
		throws DataSetExistsException {
		Utilities.checkNotNullArgument(dsId, "dataset id is null");
		Utilities.checkNotNullArgument(plan, "populating plan is null");
		
		// Plan의 마지막 operator가 store가 아닌 경우 이를 추가한다ㅏ.
		Tuple<Plan,RecordSchema> adjusted = adjustPlanForStore(plan, FOption.empty(), dsId, opts);
		
		// Plan 수행 결과 DataSet의 Schema를 계산하여 empty 데이터세트를 생성한다.
		RecordSchema outSchema = getOutputRecordSchema(plan);
		createDataSet(dsId, outSchema, opts.toCreateOptions());
		
		try {
			execute(adjusted._1, ExecutePlanOptions.DEFAULT);
			return getDataSet(dsId);
		}
		catch ( Exception e ) {
			Try.run(() -> deleteDataSet(dsId));
			Throwables.sneakyThrow(e);
			throw new AssertionError();
		}
	}

	@Override
	public DataSetImpl bindExternalDataSet(String dsId, String srcPath, DataSetType type,
											BindDataSetOptions opts) {
		return m_marmot.bindExternalDataSet(dsId, srcPath, type, opts);
	}

	@Override
	public DataSetImpl buildDataSet(String dsId, String path, String infoPath,
									BindDataSetOptions opts) {
		return m_marmot.buildDataSet(dsId, path, infoPath, opts);
	}

	@Override
	public List<String> getDirAll() {
		return m_marmot.getDirAll();
	}

	@Override
	public List<String> getSubDirAll(String folder, boolean recursive) {
		return m_marmot.getSubDirAll(folder, recursive);
	}

	@Override
	public String getParentDir(String folder) {
		return m_marmot.getParentDir(folder);
	}

	@Override
	public void moveDir(String path, String newPath) {
		m_marmot.moveDir(path, newPath);
	}

	@Override
	public void deleteDir(String folder) {
		m_marmot.deleteDir(folder);
	}

	public PlanExecution createPlanExecution(Plan plan, ExecutePlanOptions opts) {
		return m_marmot.createPlanExecution(plan, opts);
	}

	@Override
	public void execute(Plan plan, ExecutePlanOptions opts) throws MarmotExecutionException {
		m_marmot.execute(plan, opts);
	}

	public void executeInUnregistered(Plan plan, ExecutePlanOptions opts) throws MarmotExecutionException {
		m_marmot.execute(plan, opts);
	}

	@Override
	public RecordSet executeLocally(Plan plan) {
		return m_marmot.executeLocally(plan);
	}

	@Override
	public RecordSet executeLocally(Plan plan, RecordSet input) {
		return m_marmot.executeLocally(plan, input);
	}

	@Override
	public FOption<Record> executeToRecord(Plan plan, ExecutePlanOptions opts)
		throws MarmotExecutionException {
		return m_marmot.executeToRecord(plan, opts);
	}

	@Override
	public RecordSet executeToRecordSet(Plan plan, ExecutePlanOptions opts) {
		return m_marmot.executeToRecordSet(plan, opts);
	}

	@Override
	public RecordSet executeToStream(String id, Plan plan) {
		return m_marmot.executeToStream(id, plan);
	}

	@Override
	public RecordSchema getProcessOutputRecordSchema(String processId, Map<String,String> params) {
		return m_marmot.getProcessOutputRecordSchema(processId, params);
	}

	@Override
	public void executeProcess(String id, Map<String,String> params) {
		m_marmot.executeProcess(id, params);
	}

	@Override
	public RecordSchema getOutputRecordSchema(Plan plan, RecordSchema inputSchema) {
		return m_marmot.getOutputRecordSchema(plan, inputSchema);
	}

	@Override
	public RecordSchema getOutputRecordSchema(Plan plan) {
		return m_marmot.getOutputRecordSchema(plan);
	}
	


	@Override
	public MarmotExecution start(Plan plan, ExecutePlanOptions opts) {
		PlanExecution planExec = m_marmot.createPlanExecution(plan,opts);
		planExec.start();
		
		MarmotExecution exec = new AsyncMarmotExecution<>(planExec);
		m_execRegistry.register(exec);
		
		return exec;
	}

	@Override
	public MarmotExecution getMarmotExecution(String id) throws ExecutionNotFoundException {
		return m_execRegistry.getMarmotExecution(id);
	}

	@Override
	public List<MarmotExecution> getMarmotExecutionAll() {
		return m_execRegistry.getMarmotExecutionAll();
	}
	

	
	@Override
	public Set<String> getModuleAnalysisClassIdAll() {
		return MarmotModuleRegistry.get().getModuleAnalysisClassAll();
	}

	@Override
	public List<String> getModuleAnalysisParameterNameAll(String classId) {
		return MarmotModuleRegistry.get().getModuleParameterNameAll(classId);
	}

	@Override
	public Set<String> getSystemAnalysisClassIdAll() {
		return MarmotModuleRegistry.get().getModuleAnalysisClassAll();
	}

	@Override
	public List<String> getSystemAnalysisParameterNameAll(String classId) {
		return MarmotModuleRegistry.get().getModuleParameterNameAll(classId);
	}
	
	

	@Override
	public MarmotAnalysis getAnalysis(String id) throws AnalysisNotFoundException {
		return m_analysisMgr.getAnalysis(id);
	}

	@Override
	public List<MarmotAnalysis> getAnalysisAll() {
		return m_analysisMgr.getAnalysisAll();
	}

	@Override
	public MarmotAnalysis findAnalysis(String id) {
		return m_analysisMgr.findAnalysis(id);
	}

	@Override
	public CompositeAnalysis findParentAnalysis(String id) {
		return m_analysisMgr.findParentAnalysis(id);
	}

	@Override
	public List<CompositeAnalysis> getAncestorAnalysisAll(String id) {
		return m_analysisMgr.getAncestorAnalysisAll(id);
	}

	@Override
	public List<MarmotAnalysis> getDescendantAnalysisAll(String id) {
		return m_analysisMgr.getDescendantAnalysisAll(id);
	}

	@Override
	public void addAnalysis(MarmotAnalysis analysis, boolean force) {
		m_analysisMgr.addAnalysis(analysis, force);
	}

	@Override
	public void deleteAnalysis(String id, boolean recursive) {
		m_analysisMgr.deleteAnalysis(id, recursive);
	}

	@Override
	public void deleteAnalysisAll() {
		m_analysisMgr.deleteAnalysisAll();
	}

	@Override
	public MarmotExecution startAnalysis(MarmotAnalysis analysis) throws MarmotExecutionException {
		return m_analysisMgr.start(analysis);
	}

	@Override
	public void executeAnalysis(MarmotAnalysis analysis) throws MarmotExecutionException {
		m_analysisMgr.execute(analysis);
	}
	
	
	
	@Override
	public RecordSet readMarmotFile(String path) throws MarmotFileNotFoundException {
		return m_marmot.getFileServer().readMarmotFile(new Path(path));
	}

	@Override
	public long copyToHdfsFile(String path, InputStream stream, FOption<Long> blockSize,
								FOption<String> codecName) throws IOException {
		return m_marmot.getFileServer().copyToHdfsFile(path, stream, blockSize, codecName);
	}

	@Override
	public void deleteHdfsFile(String path) throws IOException {
		m_marmot.getFileServer().deleteFile(new Path(path));
	}
	

	public long getDefaultGrpMessageSize() {
		return m_marmot.getProperty("marmot.default.grpc_msg_size")
				.map(UnitUtils::parseByteSize)
				.getOrElse(DEF_GRPC_MSG_SIZE);
	}

	
	
	Tuple<Plan,RecordSchema> adjustPlanForStore(Plan plan, FOption<RecordSet> input,
												String dsId, StoreDataSetOptions opts) {
		RecordSetOperatorChain chain = input.map(RecordSet::getRecordSchema)
											.map(s -> RecordSetOperatorChain.from(m_marmot, plan, s))
											.getOrElse(() -> RecordSetOperatorChain.from(m_marmot, plan));
		if ( chain.length() == 0 ) {
			throw new IllegalArgumentException("Plan is empty");
		}
		
		// Plan의 마지막 연산이 'StoreIntoDataSet'가 아닌 경우 강제로 'StoreIntoDataSet' 연산을 추가시킨다.
		// 만일 마지막 연산이 다른 RecordSetConsumer인 경우이거나, 다른 데이터세트의 이름으로 저장하는
		// 경우라면 예외를 발생시킨다.
		//
		RecordSetOperator last = chain.getLast();
		if ( last instanceof StoreDataSet ) {
			dsId = Catalogs.normalize(dsId);
			String storeId = Catalogs.normalize(((StoreDataSet)last).getDataSetId());
			if ( storeId != null && !storeId.equals(dsId) ) {
				String msg = String.format("Plan does not store data into dataset['%s'] "
											+ "instead '%s'", dsId, storeId);
				throw new IllegalArgumentException(msg);
			}
		}
		else if ( last instanceof RecordSetConsumer ) {
			throw new IllegalArgumentException("Plan does not store into target dataset: last=" + last);
		}
		else {
			StoreDataSet store = new StoreDataSet(dsId, opts);
			chain.add(store);
		}
		
		return Tuple.of(chain.toPlan(plan.getName()), chain.getOutputRecordSchema());
	}
/*
	public static Path getConfigDir(File homeDir) {
		String confHome = new File(homeDir, "hadoop-conf").getAbsolutePath();
		
		return new Path(confHome);
	}
	
	public static Path getConfigDir() {
		String home = System.getenv("MARMOT_SERVER_HOME");
		if ( home == null ) {
			throw new IllegalStateException("Environment variable 'MARMOT_SERVER_HOME' is missing");
		}
		
		return getConfigDir(new File(home));
	}
	
	public void createKafkaTopic(String name, int nparts, int nreps, boolean force) {
		if ( !force ) {
			_createKafkaTopic(name, nparts, nreps);
		}
		else {
			RuntimeException error = null;
			for ( int i =0; i < 10; ++i ) {
				try {
					_createKafkaTopic(name, nparts, nreps);
					return;
				}
				catch ( TopicExistsException | TopicAlreadyMarkedForDeletionException e ) {
					error = e;
					deleteKafkaTopic(name);
					
					Try.run(() -> Thread.sleep(1000));
				}
			}
			if ( error != null ) {
				throw error;
			}
			else {
				throw new RuntimeException("fails to delete KafkaTopic: " + name);
			}
		}
	}
	
	private void _createKafkaTopic(String name, int nparts, int nreps) {
		String zkHosts = getZooKeeperHosts();
	
		int sessionTimeout = 15 * 1000;
		int connectionTimeout = 10 * 1000;
		
		ZkUtils utils = null;
		try {
			utils = ZkUtils.apply(zkHosts, sessionTimeout, connectionTimeout, false);
			AdminUtils.createTopic(utils, name, nparts, nreps, new Properties());
		}
		finally {
			if ( utils != null ) {
				utils.close();
			}
		}
	}
	
	public void deleteKafkaTopic(String name) {
		String zkHosts = getZooKeeperHosts();
	
		ZkClient client = new ZkClient(zkHosts);
		try {
			ZkUtils utils = new ZkUtils(client, new ZkConnection(zkHosts), false);
			AdminUtils.deleteTopic(utils, name);
		}
		finally {
			client.close();
		}
	}
	
	public KafkaConsumer<Long, byte[]> getKafkaConsumer(String group) {
		Properties configs = new Properties();
		configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokerList());
		configs.put(ConsumerConfig.GROUP_ID_CONFIG, group);
		configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
									ByteArrayDeserializer.class.getName());
		
		return new KafkaConsumer<>(configs);
	}

	public RecordSchema getMarmotFileRecordSchema(String path) {
		Utilities.checkNotNullArgument(path, "MarmotFile path is null");
		
		LoadMarmotFile load = LoadMarmotFile.from(path);
		load.initialize(this);
		return load.getRecordSchema();
	}
*/
}
