package marmot;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.FactoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.forcewave.ghdfs.obj.GMetaObject;

import marmot.dataset.Catalog;
import marmot.dataset.Catalogs;
import marmot.dataset.DataSet;
import marmot.dataset.DataSetExistsException;
import marmot.dataset.DataSetImpl;
import marmot.dataset.DataSetNotFoundException;
import marmot.dataset.DataSetType;
import marmot.dataset.GeometryColumnInfo;
import marmot.dataset.MarmotFileServer;
import marmot.dataset.SpatialIndexCatalogInfo;
import marmot.exec.MarmotExecutionException;
import marmot.exec.PlanExecution;
import marmot.exec.PlanExecutionFactory;
import marmot.geo.CRSUtils;
import marmot.geo.catalog.DataSetInfo;
import marmot.geowave.LoadGHdfsFile;
import marmot.io.HdfsPath;
import marmot.io.MarmotSequenceFile;
import marmot.kafka.KafkaTopicRecordSet;
import marmot.mapreduce.MultiJobPlanExecution;
import marmot.mapreduce.MultiStageMRExecutionFactory;
import marmot.mapreduce.support.ClusterMetrics;
import marmot.module.MarmotModule;
import marmot.module.MarmotModuleRegistry;
import marmot.optor.CreateDataSetOptions;
import marmot.optor.LoadCustomTextFile;
import marmot.optor.LoadMarmotFile;
import marmot.optor.RecordSetConsumer;
import marmot.optor.RecordSetLoader;
import marmot.optor.RecordSetOperator;
import marmot.optor.StoreAsHeapfile;
import marmot.proto.MarmotCoreProto;
import marmot.remote.protobuf.PBSerializableRegistry;
import marmot.support.PBException;
import marmot.support.PBSerializable;
import marmot.support.RecordSetOperatorChain;
import utils.StopWatch;
import utils.Throwables;
import utils.UnitUtils;
import utils.Utilities;
import utils.func.FOption;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotCore implements PBSerializable<MarmotCoreProto>, Serializable {
	private static final long serialVersionUID = -8990345856034899604L;
	private final static Logger s_logger = LoggerFactory.getLogger(MarmotCore.class);

	private static final long DEF_TEXT_BLOCK_SIZE = UnitUtils.parseByteSize("128m");
	private static final long DEF_BLOCK_SIZE = UnitUtils.parseByteSize("64m");
	private static final long DEF_CLUSTER_SIZE = UnitUtils.parseByteSize("64m");
	private static final long DEF_CLUSTER_CACHE_SIZE = UnitUtils.parseByteSize("192m");
	private static final int DEF_PARTITION_COUNT = 7;
	private static final String DEF_MAP_OUTPUT_CODEC_NAME = "snappy";
	private static final String PROP_MAP_OUTPUT_COMPRESS = "marmot.map.output.compress";
	private static final String PROP_MAP_OUTPUT_COMPRESS_CODEC = "marmot.map.output.compress.codec.name";
	
	private JobContext m_jobContext;
	private Configuration m_conf;
	private FileSystem m_fs;
	private MarmotFileServer m_fileServer;
	private Catalog m_catalog;
	private PlanExecutionFactory m_planExecFact;
	
	private ClusterMetrics s_metrics;
	private String m_rmWebAppAddress;
	private int m_maxBlocksPerSplit = 4;//(1024 / 64);
	private int m_locallyExecutableBlockLimitSoft = 1;
	private int m_locallyExecutableBlockLimitHard = 3;
	private FOption<String> m_defaultMapOutputCompressCodecName;
	
	public MarmotCore(JobContext jcontext) {
		this(jcontext.getConfiguration());
		
		m_jobContext = jcontext;
	}
	
	public MarmotCore(Configuration conf) {
		initialize(conf, null);
	}
	
	public MarmotCore(Configuration conf, PlanExecutionFactory planFact) {
		initialize(conf, planFact);
	}
	
	private void initialize(Configuration conf, PlanExecutionFactory planFact) {
		m_conf = conf;
		m_fileServer = new MarmotFileServer(this, conf);
		m_fs = HdfsPath.getFileSystem(conf);
		m_catalog = Catalog.initialize(conf);
		m_planExecFact = (planFact != null) ? planFact: new MultiStageMRExecutionFactory(this);
		
		m_rmWebAppAddress = conf.get("yarn.resourcemanager.webapp.address");
		m_defaultMapOutputCompressCodecName = loadDefaultMapOutputCompressCodecName();
		
		PBSerializableRegistry.bindAll();
		if ( s_logger.isInfoEnabled() ) {
			s_logger.info("defaults: block_size={}, text_block_size={}, cluster_size={}, "
						+ "cluster_cache_size={}, reducer_count={}",
						UnitUtils.toByteSizeString(getDefaultMarmotBlockSize()),
						UnitUtils.toByteSizeString(getDefaultTextBlockSize()),
						UnitUtils.toByteSizeString(getDefaultClusterSize()),
						UnitUtils.toByteSizeString(getDefaultClusterCacheSize()),
						getDefaultPartitionCount());
		}
	}
	
	/**
	 * 카다로그를 반환한다.
	 * 
	 * @return	카다로그
	 */
	public Catalog getCatalog() {
		return m_catalog;
	}
	
	/**
	 * 하둡 설정을 반환한다.
	 * 
	 * @return	설정 객체.
	 */
	public Configuration getHadoopConfiguration() {
		return m_conf;
	}
	
	/**
	 * HDFS 파일 시스템 객체를 반환한다.
	 * 
	 * @return	HDFS 파일 시스템
	 */
	public FileSystem getHadoopFileSystem() {
		return m_fs;
	}
	
	public JobContext getJobContext() {
		return m_jobContext;
	}
	
	public MarmotFileServer getFileServer() {
		return m_fileServer;
	}
	
	
	/**
	 * 식별자에 해당하는 데이터세트 객체를 반환한다.
	 * <p>
	 * 식별자에 해당하는 데이터세트가 없는 경우에는 {@link DataSetNotFoundException} 예외를 발생시킨다.
	 * 
	 * @param dsId	데이터세트 식별자.
	 * @return	데이터세트 객체
	 * @throws DataSetNotFoundException	식별자에 해당하는 데이터세트가 없는 경우
	 */
	public DataSetImpl getDataSet(String dsId) {
		Utilities.checkNotNullArgument(dsId);
		
		return new DataSetImpl(this, getDataSetInfo(dsId));
	}
	
	/**
	 * 식별자에 해당하는 데이터세트 객체를 반환한다.
	 * <p>
	 * 식별자에 해당하는 데이터세트가 없는 경우에는 null을 반환한다.
	 * 
	 * @param dsId	데이터세트 식별자.
	 * @return	데이터세트 객체
	 */
	public DataSetImpl getDataSetOrNull(String dsId) {
		Utilities.checkNotNullArgument(dsId);
		
		return m_catalog.getDataSetInfo(dsId)
						.map(info -> new DataSetImpl(this, info))
						.getOrNull();
	}
	
	/**
	 * 식별자에 해당하는 데이터세트 등록정보를 반환한다.
	 * <p>
	 * 식별자에 해당하는 데이터세트가 없는 경우에는
	 * {@link DataSetNotFoundException} 예외를 발생시킨다.
	 * 
	 * @param dsId	데이터세트 식별자.
	 * @return	데이터세트 등록정보 객체
	 * @throws DataSetNotFoundException	식별자에 해당하는 데이터세트 등록정보가 없는 경우
	 */
	public DataSetInfo getDataSetInfo(String dsId) {
		Utilities.checkNotNullArgument(dsId);
		
		return m_catalog.getDataSetInfo(dsId)
						.getOrThrow(()->new DataSetNotFoundException(dsId));
	}

	/**
	 * Marmot에 등록된 모든 데이터세트들의 리스트를 반환한다.
	 * 
	 * @return	데이터세트 리스트
	 */
	public List<DataSet> getDataSetAll() {
		return FStream.from(m_catalog.getDataSetInfoAll())
						.map(info -> (DataSet)new DataSetImpl(this, info))
						.toList();
	}

	/**
	 * 주어진 폴더하에 존재하는 데이터세트들의 리시트를 반환한다.
	 * 
	 * @param folder	대상 폴더 경로명
	 * @param recursive	모든 하위 폴더 검색 여부
	 * @return	데이터세트 리스트
	 */
	public List<DataSet> getDataSetAllInDir(String folder, boolean recursive) {
		Utilities.checkNotNullArgument(folder);
		
		return FStream.from(m_catalog.getDataSetInfoAllInDir(folder, recursive))
						.map(info -> (DataSet)new DataSetImpl(this, info))
						.toList();
	}

	/**
	 * 빈 데이터세트를 생성한다.
	 * 
	 * @param dsId	생성될 데이터세트의 식별자.
	 * @param schema	생성될 데이터세트의 스키마.
	 * @param opts	생성 옵션
	 * @return	생성된 데이터세트 객체
	 */
	public DataSetImpl createDataSet(String dsId, RecordSchema schema, CreateDataSetOptions opts)
		throws DataSetExistsException {
		Utilities.checkNotNullArgument(dsId, "dataset id is null");
		Utilities.checkNotNullArgument(schema, "dataset schema is null");
		Utilities.checkNotNullArgument(opts, "CreateDataSetOptions is null");
		
		// 'force' 옵션이 있는 경우는 식별자에 해당하는 미리 삭제한다.
		// 주어진 식별자가 폴더인 경우는 폴더 전체를 삭제한다.
		if ( opts.force() ) {
			deleteDir(dsId);
			deleteDataSet(dsId);
		}
		
		// 데이터세트 관련 정보를 카다로그에 추가시킨다.
		DataSetInfo dsInfo = new DataSetInfo(dsId, opts.type(), schema);
		dsInfo.setGeometryColumnInfo(opts.geometryColumnInfo());
		dsInfo.setBlockSize(opts.blockSize()
								.getOrElse(() -> getDefaultBlockSize(DataSetType.FILE)));
		dsInfo.setCompressionCodecName(opts.compressionCodecName());
		dsInfo.setUpdatedMillis(System.currentTimeMillis());
		m_catalog.initialize(dsInfo);
		m_catalog.insertDataSetInfo(dsInfo);
		
		return new DataSetImpl(this, dsInfo);
	}
	
	public DataSetImpl buildDataSet(String dsId, String path, String infoPath, BindDataSetOptions opts) {
		// 'force' 옵션이 있는 경우는 식별자에 해당하는 미리 삭제한다.
		// 주어진 식별자가 폴더인 경우는 폴더 전체를 삭제한다.
		if ( opts.force() ) {
			deleteDir(dsId);
			deleteDataSet(dsId);
		}

		FileSystem fs = getHadoopFileSystem();
		HdfsPath fromPath = HdfsPath.of(fs, new Path(path));
		HdfsPath dsPath = HdfsPath.of(fs, m_catalog.generateFilePath(dsId));
		MarmotSequenceFile msf = MarmotSequenceFile.of(fromPath);
		RecordSchema schema = msf.getRecordSchema();
		
		// 데이터세트 관련 정보를 카다로그에 추가시킨다.
		DataSetInfo dsInfo = new DataSetInfo(dsId, DataSetType.FILE, schema);
		dsInfo.setFilePath(dsPath.toString());
		dsInfo.setBlockSize(msf.getBlockSize());
		dsInfo.setUpdatedMillis(System.currentTimeMillis());
		
		opts.geometryColumnInfo().ifPresent(gcInfo -> {
			Envelope mbr = new Envelope();
			long count = 0;
			
			MarmotSequenceFile infoMsf = MarmotSequenceFile.of(HdfsPath.of(fs, new Path(infoPath)));
			try ( RecordSet rset = infoMsf.read() ) {
				for ( Record record: rset ) {
					mbr.expandToInclude((Envelope)record.get(0));
					count += record.getLong(1);
				}
			}
			dsInfo.setGeometryColumnInfo(opts.geometryColumnInfo());
			dsInfo.setRecordCount(count);
			dsInfo.setBounds(mbr);
		});
		m_catalog.initialize(dsInfo);
		m_catalog.insertDataSetInfo(dsInfo);
		
		fromPath.moveTo(dsPath.child(UUID.randomUUID().toString()));
		
		return new DataSetImpl(this, dsInfo);
	}
	
	public DataSetImpl bindExternalDataSet(String dsId, String path, DataSetType type,
											BindDataSetOptions opts) {
		DataSetInfo info = (type == DataSetType.GWAVE)
						? bindGeoWaveDataSet(dsId, path, opts)
						: bindDefaultDataSet(dsId, path, type, opts);
		if ( opts.force() ) {
			deleteDataSet(dsId);
		}
		
		m_catalog.insertDataSetInfo(info);
		
		return new DataSetImpl(this, getDataSetInfo(dsId));
	}
	
	private DataSetInfo bindDefaultDataSet(String dsId, String path, DataSetType type,
											BindDataSetOptions opts) {
		RecordSetLoader load;
		switch ( type ) {
			case TEXT:
				load = new LoadCustomTextFile(path);
				break;
			case FILE:
			case LINK:
				load = LoadMarmotFile.from(path);
				break;
			case GWAVE:
				throw new AssertionError("should not be called: type=" + type);
			default:
				throw new RecordSetException("unsupported DataSet type: " + type);
		}
		load.initialize(this);
		
		RecordSchema schema = load.getRecordSchema();
		DataSetInfo info = new DataSetInfo(dsId, type, schema);
		opts.geometryColumnInfo().ifPresent(gcInfo -> {
			String geomCol = gcInfo.name();
			
			Column found = schema.findColumn(geomCol)
								.getOrThrow(() -> 
										new IllegalArgumentException("Geometry column not found: "
												+ "name=" + geomCol));
			if ( !found.type().isGeometryType() ) {
				throw new IllegalArgumentException("column is not Geometry type: "
													+ "name=" + geomCol);
			}
			
			info.setGeometryColumnInfo(gcInfo);
		});
		info.setFilePath(path);
		info.setUpdatedMillis(System.currentTimeMillis());
		
		return info;
	}

	private DataSetInfo bindGeoWaveDataSet(String dsId, String layerName, BindDataSetOptions opts) {
		try {
			LoadGHdfsFile load = LoadGHdfsFile.from(layerName);
			load.initialize(this);
			
			GMetaObject gmeta = load.getGMetaObject();
			RecordSchema schema = load.getRecordSchema();

			DataSetInfo info = new DataSetInfo(dsId, DataSetType.GWAVE, schema);
			info.setFilePath("/tmp/ghdfs/" + layerName);
			info.setBounds(gmeta.getEnvelope());
			String srid = CRSUtils.toEPSG(gmeta.getCrs());
			info.setGeometryColumnInfo(new GeometryColumnInfo("shape", srid));
			info.setRecordCount(gmeta.getTotalCount());
			info.setUpdatedMillis(System.currentTimeMillis());
			
			return info;
		}
		catch ( FactoryException e ) {
			throw new MarmotInternalException(e);
		}
	}

	public boolean deleteDataSet(String dsId) {
		String hdfsFilePath;
		
		DataSetInfo info = m_catalog.getDataSetInfo(dsId).getOrNull();
		if ( info != null ) {
			// Thumbnail이 존재하면 삭제한다.
			DataSetImpl ds = new DataSetImpl(this, info);
			ds.deleteThumbnail();
			
			// 인덱스가 존재하면 해당 인덱스도 제거한다.
			ds.deleteSpatialIndex();
			
			m_catalog.deleteDataSetInfo(dsId);
			hdfsFilePath = info.getFilePath();

			switch ( info.getType() ) {
				case FILE:
				case SPATIAL_CLUSTER:
					getFileServer().deleteFileUpward(new Path(hdfsFilePath));
				default:
			}
//			if ( !m_catalog.isExternalDataSet(hdfsFilePath)
//				&& !info.getType().equals(DataSetType.LINK) ) {
//				getFileServer().deleteFileUpward(new Path(hdfsFilePath));
//			}
		}
		else {
			// catalog에 등록되어 있지 않아도, HDFS에 파일이 존재할 수 있음
			hdfsFilePath = m_catalog.generateFilePath(dsId).toString();
			getFileServer().deleteFileUpward(new Path(hdfsFilePath));
		}
		
		return true;
	}

	public void moveDataSet(String id, String newId) {
		Catalog catalog = m_catalog;
		
		DataSetInfo info = catalog.getDataSetInfo(id)
								.getOrThrow(()->new DataSetNotFoundException(id));
		
		if ( catalog.getDataSetInfo(newId).isPresent() ) {
			throw new DataSetExistsException("target dataset exists: id=" + newId);
		}
		if ( catalog.isDirectory(newId) ) {
			String name = Catalogs.getName(id);
			newId = Catalogs.toDataSetId(newId, name);
		}
		final String newDsId = newId;
		
		// 인덱스가 존재하면 해당 부분도 수정한다.
		catalog.getSpatialIndexCatalogInfo(id).ifPresent(idxInfo -> {
			GeometryColumnInfo gcInfo = idxInfo.getGeometryColumnInfo();
			String idxPathStr = catalog.generateSpatialIndexPath(id, gcInfo.name()).toString();
			SpatialIndexCatalogInfo newInfo = new SpatialIndexCatalogInfo(newDsId, gcInfo, idxPathStr);
			
			catalog.deleteSpatialIndexCatalogInfo(idxInfo.getDataSetId(),
												idxInfo.getGeometryColumnInfo().name());
			catalog.insertSpatialIndexCatalogInfo(newInfo);
			
			HdfsPath srcIdxPath = HdfsPath.of(m_conf, new Path(idxInfo.getHdfsFilePath()));
			HdfsPath dstIdxPath = HdfsPath.of(m_conf, new Path(idxPathStr));
			srcIdxPath.moveTo(dstIdxPath);
		});
		
		String newFilePath;
		if ( !catalog.isExternalDataSet(info.getFilePath()) ) {
			HdfsPath srcPath = HdfsPath.of(m_conf, new Path(info.getFilePath()));
			newFilePath = catalog.generateFilePath(newId).toString();
			srcPath.moveTo(HdfsPath.of(m_conf, new Path(newFilePath)));
		}
		else {
			newFilePath = info.getFilePath();
		}
		
		catalog.deleteDataSetInfo(info.getId());
		DataSetInfo newInfo = new DataSetInfo(newId, info.getType(), info.getRecordSchema());
		newInfo.setGeometryColumnInfo(info.getGeometryColumnInfo());
		newInfo.setBounds(info.getBounds());
		newInfo.setRecordCount(info.getRecordCount());
		newInfo.setFilePath(newFilePath);
		catalog.insertOrReplaceDataSetInfo(newInfo);
	}

	public List<String> getDirAll() {
		return m_catalog.getDirAll();
	}

	public List<String> getSubDirAll(String folder, boolean recursive) {
		return m_catalog.getSubDirAll(folder, recursive);
	}

	public String getParentDir(String folder) {
		return m_catalog.getParentDir(folder);
	}

	public void moveDir(String path, String newPath) {
		String prefix = Catalogs.normalize(path);
		if ( !prefix.endsWith("/") ) {
			prefix = prefix + "/";
		}
		int prefixLen = prefix.length();
		
		newPath = Catalogs.normalize(newPath);
		for ( DataSetInfo info: m_catalog.getDataSetInfoAllInDir(path, true) ) {
			String suffix = info.getId().substring(prefixLen);
			String newId = newPath + Catalogs.ID_DELIM + suffix;
			
			moveDataSet(info.getId(), newId);
		}
	}

	public void deleteDir(String folder) {
		List<DataSetInfo> infoList = m_catalog.getDataSetInfoAllInDir(folder, true);
		
		infoList.stream()
				.sorted((i1,i2) -> i1.getId().compareTo(i2.getId()))
				.forEach(info -> {
					String path = info.getFilePath();
					if ( !m_catalog.isExternalDataSet(path) ) {
						deleteDataSet(info.getId());
					}
				});
		m_catalog.deleteDir(folder);
	}
	


	public RecordSchema getOutputRecordSchema(Plan plan, RecordSchema inputSchema) {
		Utilities.checkNotNullArgument(plan, "plan is null");
		Utilities.checkNotNullArgument(inputSchema, "inputSchema is null");
		
		return RecordSetOperatorChain.from(this, plan, inputSchema)
										.getOutputRecordSchema();
	}

	public RecordSchema getOutputRecordSchema(Plan plan) {
		return getOutputRecordSchema(plan, RecordSchema.NULL);
	}


	public PlanExecution createPlanExecution(Plan plan, ExecutePlanOptions opts) {
		PlanExecution exec = m_planExecFact.create(plan);
		if ( opts.disableLocalExecution().getOrElse(false) ) {
			exec.setDisableLocalExecution(true);
		}
		
		opts.mapOutputCompressionCodec()
			.ifPresent(exec::setMapOutputCompressCodec);
		
		return exec;
	}
	
	public void execute(String jobName, RecordSetOperatorChain optorChain) throws MarmotExecutionException {
		PlanExecution planExec = MultiJobPlanExecution.create(this, jobName, optorChain);
		try {
			planExec.run();
		}
		catch ( InterruptedException e ) {
			throw new CancellationException();
		}
		catch ( CancellationException e ) {
			throw e;
		}
		catch ( Throwable e ) {
			Throwable cause = unwrapThrowable(e);
			Throwables.throwIfInstanceOf(cause, MarmotExecutionException.class);
			throw new MarmotExecutionException(cause);
		}
	}

	public void execute(Plan plan) throws MarmotExecutionException {
		execute(plan, ExecutePlanOptions.DEFAULT);
	}

	public void execute(Plan plan, ExecutePlanOptions opts) throws MarmotExecutionException {
		PlanExecution planExec = createPlanExecution(plan, opts);
		try {
			planExec.run();
		}
		catch ( InterruptedException e ) {
			throw new CancellationException();
		}
		catch ( CancellationException e ) {
			throw e;
		}
		catch ( Throwable e ) {
			Throwable cause = unwrapThrowable(e);
			Throwables.throwIfInstanceOf(cause, MarmotExecutionException.class);
			throw new MarmotExecutionException(cause);
		}
	}

	public RecordSet executeLocally(Plan plan) {
		return RecordSetOperatorChain.from(this, plan).run();
	}

	public RecordSet executeLocally(Plan plan, RecordSet input) {
		Utilities.checkNotNullArgument(plan, "plan is null");
		Utilities.checkNotNullArgument(input, "input RecordSet is null");
		
		RecordSetOperatorChain chain = RecordSetOperatorChain.from(this, plan,
																	input.getRecordSchema());
		if ( chain.getRecordSetLoader() != null ) {
			throw new IllegalArgumentException("plan has RecordSetLoader: plan=" + plan);
		}
		
		return chain.run(input);
	}
	
	public FOption<Record> executeToRecord(Plan plan) throws MarmotExecutionException {
		return executeToRecord(plan, ExecutePlanOptions.DEFAULT);
	}

	public FOption<Record> executeToRecord(Plan plan, ExecutePlanOptions opts)
		throws MarmotExecutionException {
		Utilities.checkNotNullArgument(plan, "plan is null");

		RecordSetOperatorChain chain = RecordSetOperatorChain.from(this, plan);
		
		// Plan의 마지막 연산으로 'StoreAsHeapfile' 연산을 추가시킨다.
		// 만일 마지막 연산이 다른 RecordSetConsumer인 경우이거나, 다른 데이터세트의 이름으로 저장하는
		// 경우라면 예외를 발생시킨다.
		//
		RecordSetOperator last = chain.getLast();
		if ( last instanceof RecordSetConsumer ) {
			throw new IllegalArgumentException("last operator should not be a consumer: last=" + last);
		}
		
		Path path = new Path("tmp/single/" + UUID.randomUUID());
		StoreAsHeapfile store = new StoreAsHeapfile(path);
		chain.add(store);
		plan = chain.toPlan(plan.getName());
		
		try {
			execute(plan, opts);
			return FOption.ofNullable(getFileServer().readMarmotFile(path).findFirst());
		}
		finally {
			getFileServer().deleteFile(path);
		}
	}

	public RecordSet executeToRecordSet(Plan plan) {
		return executeToRecordSet(plan, ExecutePlanOptions.DEFAULT);
	}

	public RecordSet executeToRecordSet(Plan plan, ExecutePlanOptions opts) {
		Utilities.checkNotNullArgument(plan, "plan is null");
		
		// 'disableLocalExecution'이 설정되지 않은 상태에서, local execution이 가능하면
		// 바로 local하게 수행시킨다.
		if ( !opts.disableLocalExecution().getOrElse(false) ) {
			if ( MultiJobPlanExecution.create(this, plan).isLocallyExecutable() ) {
				return executeLocally(plan);
			}
		}

		RecordSetOperatorChain chain = RecordSetOperatorChain.from(this, plan);
		
		// Plan의 마지막 연산으로 'StoreAsHeapfile' 연산을 추가시킨다.
		// 만일 마지막 연산이 다른 RecordSetConsumer인 경우이거나, 다른 데이터세트의 이름으로 저장하는
		// 경우라면 예외를 발생시킨다.
		//
		RecordSetOperator last = chain.getLast();
		if ( last instanceof RecordSetConsumer ) {
			throw new IllegalArgumentException("last operator should not be a consumer: last=" + last);
		}
		
		Path path = new Path("tmp/rset/" + UUID.randomUUID());
		StoreAsHeapfile store = new StoreAsHeapfile(path);
		chain.add(store);
		plan = chain.toPlan(plan.getName());

		MarmotFileServer mfs = getFileServer();
		try {
			execute(plan, opts);
			RecordSet rset = mfs.readMarmotFile(path);
			return rset.onClose(() -> mfs.deleteFile(path));
		}
		catch ( Throwable e ) {
			mfs.deleteFile(path);
			
			Throwables.sneakyThrow(e);
			throw new AssertionError();
		}
	}

	/**
	 * 주어진 Plan을 수행시키고, 그 결과를 반환한다.
	 * <p>
	 * Plan 수행 결과로 생성된 결과 레코드세트의 첫번째 레코드의 첫번째 컬럼 값을
	 * {@link Long} 형식으로 변환하여 반환한다.
	 * 일반적으로 본 메소드는 plan 수행 결과로 단일 컬럼으로 구성된 단일 레코드가
	 * 생성되는 경우 주로 사용된다.
	 * 
	 * @param plan	수행시킬 실행 계획.
	 * @param opts	실행 계획 옵션
	 * @return	Plan 수행 결과로 생성된 레코드세트의 첫번째 레코드의 첫번째 컬럼 값.
	 * 				만일 결과 레코드가 생성되지 않은 경우에는 {@link FOption#empty()}.
	 */
	public FOption<Long> executeToLong(Plan plan, ExecutePlanOptions opts)
		throws MarmotExecutionException {
		return executeToRecord(plan, opts).map(r -> r.getLong(0));
	}
	
	public FOption<Long> executeToLong(Plan plan) throws MarmotExecutionException {
		return executeToLong(plan, ExecutePlanOptions.DEFAULT);
	}

	/**
	 * 주어진 Plan을 수행시키고, 그 결과를 반환한다.
	 * <p>
	 * Plan 수행 결과로 생성된 결과 레코드세트의 첫번째 레코드의 첫번째 컬럼 값을
	 * {@link Geometry} 형식으로 변환하여 반환한다.
	 * 일반적으로 본 메소드는 plan 수행 결과로 단일 컬럼으로 구성된 단일 레코드가
	 * 생성되는 경우 주로 사용된다.
	 * 
	 * @param plan	수행시킬 실행 계획.
	 * @param opts	실행 계획 옵션
	 * @return	Plan 수행 결과로 생성된 레코드세트의 첫번째 레코드의 첫번째 컬럼 값.
	 * 				만일 결과 레코드가 생성되지 않은 경우에는 {@link FOption#empty()}.
	 */
	public FOption<Geometry> executeToGeometry(Plan plan, ExecutePlanOptions opts)
		throws MarmotExecutionException {
		return executeToRecord(plan, opts).map(r -> r.getGeometry(0));
	}
	public FOption<Geometry> executeToGeometry(Plan plan) throws MarmotExecutionException {
		return executeToGeometry(plan, ExecutePlanOptions.DEFAULT);
	}

	public RecordSet executeToStream(String id, Plan plan) {
		Utilities.checkNotNullArgument(id, "stream id is null");
		Utilities.checkNotNullArgument(plan, "plan is null");
		
		return new KafkaTopicRecordSet(this, plan, id);
	}

	public RecordSchema getProcessOutputRecordSchema(String processId, Map<String,String> params) {
		MarmotModule proc = MarmotModuleRegistry.get().createModule(processId);
		return proc.getOutputRecordSchema(this, params);
	}
	
	public void executeProcess(String id, Map<String,String> params) {
		StopWatch watch = StopWatch.start();
		
		MarmotModule proc = MarmotModuleRegistry.get().createModule(id);
		proc.initialize(this, params);
		
		s_logger.debug("executing process: {}", proc);
		proc.run();
		
		s_logger.info("executed process: {}, elapsed={}",
						proc, watch.stopAndGetElpasedTimeString());
	}
	
	

	
	public FOption<String> getProperty(String name) {
		return FOption.ofNullable(m_conf.get(name, null));
	}
	
	public void setProperty(String name, String value) {
		m_conf.set(name, value);
	}
	
	public long getDefaultBlockSize(DataSetType type) {
		switch ( type ) {
			case FILE:
			case SPATIAL_CLUSTER:
				return getDefaultMarmotBlockSize();
			case TEXT:
			case LINK:
				return getDefaultTextBlockSize();
			case CLUSTER:
				return getDefaultClusterSize();
			default:
				throw new AssertionError();
		}
	}
	
	public long getDefaultMarmotBlockSize() {
		return getProperty("marmot.default.block_size")
				.map(UnitUtils::parseByteSize)
				.getOrElse(DEF_BLOCK_SIZE);
	}
	
	public long getDefaultTextBlockSize() {
		return getProperty("marmot.default.text_block_size")
				.map(UnitUtils::parseByteSize)
				.getOrElse(DEF_TEXT_BLOCK_SIZE);
	}
	
	public long getDefaultClusterSize() {
		return getProperty("marmot.default.cluster_size")
				.map(UnitUtils::parseByteSize)
				.getOrElse(DEF_CLUSTER_SIZE);
	}
	
	public long getDefaultClusterCacheSize() {
		return getProperty("marmot.default.cluster_cache_size")
				.map(UnitUtils::parseByteSize)
				.getOrElse(DEF_CLUSTER_CACHE_SIZE);
	}

	public int getDefaultPartitionCount() {
		return getProperty("marmot.default.reducer_count")
				.map(Integer::parseInt)
				.getOrElse(DEF_PARTITION_COUNT);
	}

	public FOption<Integer> getProgressReportIntervalSeconds() {
		return getProperty("marmot.default.progress_report_interval")
				.map(UnitUtils::parseDurationMillis)
				.map(millis -> (int)TimeUnit.MILLISECONDS.toSeconds(millis));
	}

	public String getZooKeeperHosts() {
		return getProperty("marmot.zookeeper.hosts")
				.getOrNull();
	}

	public String getKafkaBrokerList() {
		return getProperty("marmot.kafka.brokers")
				.getOrNull();
	}
	
	public ClusterMetrics getClusterMetric() {
		return (s_metrics != null ) ? s_metrics
									: (s_metrics=ClusterMetrics.get(m_rmWebAppAddress));
	}
	
	public int getLocallyExecutableBlockLimitSoft() {
		return m_locallyExecutableBlockLimitSoft;
	}
	
	public long getlocallyExecutableBlockLimitHard() {
		return m_locallyExecutableBlockLimitHard;
	}
	
	public int getMaxBlockPerSplit() {
		return m_maxBlocksPerSplit;
	}
	
	private FOption<String> loadDefaultMapOutputCompressCodecName() {
		if ( m_conf.getBoolean(PROP_MAP_OUTPUT_COMPRESS, true) ) {
			String codec = m_conf.get(PROP_MAP_OUTPUT_COMPRESS_CODEC);
			return FOption.ofNullable(codec)
							.orElse(DEF_MAP_OUTPUT_CODEC_NAME);
		}
		else {
			return FOption.empty();
		}
	}
	
	public FOption<String> getDefaultMapOutputCompressCodecName() {
		return m_defaultMapOutputCompressCodecName;
	}
	
	public static Path getConfigDir(File homeDir) {
		String confHome = new File(homeDir, "hadoop-conf").getAbsolutePath();
		
		return new Path(confHome);
	}
	
	public static MarmotCore fromProto(MarmotCoreProto proto) {
		byte[] bytes = proto.getXmlString().getBytes(StandardCharsets.UTF_8);
		ByteArrayInputStream baos = new ByteArrayInputStream(bytes);
		
		Configuration conf = new Configuration();
		conf.addResource(baos);
		
		return new MarmotCore(conf);
	}

	@Override
	public MarmotCoreProto toProto() {
		try {
			StringWriter writer = new StringWriter();
			m_conf.writeXml(writer);
			
			return MarmotCoreProto.newBuilder()
									.setXmlString(writer.toString())
									.build();
		}
		catch ( IOException e ) {
			throw new PBException(e);
		}
	}
	
	private Object writeReplace() {
		return new SerializationProxy(this);
	}
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Use Serialization Proxy instead.");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = -968376753860982266L;
		
		private final MarmotCoreProto m_proto;
		
		private SerializationProxy(MarmotCore marmot) {
			m_proto = marmot.toProto();
		}
		
		private Object readResolve() {
			return MarmotCore.fromProto(m_proto);
		}
	}
	
	static Throwable unwrapThrowable(Throwable e) {
		Throwable cause = Throwables.unwrapThrowable(e);
		if ( cause instanceof MarmotExecutionException ) {
			Throwable c2 = ((MarmotExecutionException)cause).getCause();
			if ( c2 != null ) {
				cause = c2;
			}
		}
		
		return cause;
	}
}
