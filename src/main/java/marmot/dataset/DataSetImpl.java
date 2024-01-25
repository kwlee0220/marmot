package marmot.dataset;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.GRecordSchema;
import marmot.MarmotCore;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSets.CountingRecordSet;
import marmot.analysis.module.geo.CreateSpatialIndexParameters;
import marmot.geo.CoordinateTransform;
import marmot.geo.catalog.DataSetInfo;
import marmot.geo.catalog.SpatialIndexInfo;
import marmot.geo.command.ClusterSpatiallyOptions;
import marmot.geo.command.CreateSpatialIndexOptions;
import marmot.geo.command.EstimateQuadKeysOptions;
import marmot.geo.query.RangeQueryEstimate;
import marmot.geo.query.RangeQueryEstimate.ClusterEstimate;
import marmot.io.HdfsPath;
import marmot.io.MarmotFileNotFoundException;
import marmot.io.geo.cluster.CacheableQuadCluster;
import marmot.io.geo.cluster.InMemoryIndexedClusterBuilder;
import marmot.io.geo.cluster.QuadCluster;
import marmot.io.geo.cluster.QuadClusterFile;
import marmot.io.geo.cluster.SpatialClusterFile;
import marmot.io.geo.index.GlobalIndexEntry;
import marmot.io.geo.index.SpatialIndexedCluster;
import marmot.io.geo.index.SpatialIndexedFile;
import marmot.module.geo.CreateSpatialIndex;
import marmot.optor.CreateDataSetOptions;
import marmot.optor.LoadDataSet;
import marmot.optor.Project;
import marmot.optor.StoreDataSet;
import marmot.optor.StoreDataSetOptions;
import marmot.optor.geo.advanced.CreateThumbnail;
import marmot.optor.geo.cluster.MBRTaggedOpaqueTransform;
import marmot.optor.geo.cluster.SplitQuadSpace;
import marmot.plan.Group;
import marmot.plan.LoadOptions;
import marmot.support.EnvelopeTaggedRecord;
import utils.Throwables;
import utils.UnitUtils;
import utils.Utilities;
import utils.func.FOption;
import utils.func.UncheckedFunction;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DataSetImpl implements DataSet {
	private static final Logger s_logger = LoggerFactory.getLogger(DataSetImpl.class);
	static final String THUMBNAIL_PREFIX = "database/thumbnails/";
	
	private final MarmotCore m_marmot;
	private final Catalog m_catalog;
	private DataSetInfo m_info;		// append() 호출시 변경될 수 있음

	private volatile CoordinateTransform m_trans = null;
	private volatile CoordinateTransform m_rtrans = null;
	
	public DataSetImpl(MarmotCore marmot, DataSetInfo info) {
		m_marmot = marmot;
		m_catalog = marmot.getCatalog();
		m_info = info;
	}
	
	public DataSetInfo getDataSetInfo() {
		return m_info;
	}

	@Override
	public String getId() {
		return m_info.getId();
	}
	
	public MarmotCore getMarmotCore() {
		return m_marmot;
	}

	@Override
	public DataSetType getType() {
		return m_info.getType();
	}
	
	@Override
	public String getDirName() {
		return m_info.getDirName();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_info.getRecordSchema();
	}
	public GRecordSchema getGRecordSchema() {
		return new GRecordSchema(m_info.getGeometryColumnInfo(), m_info.getRecordSchema());
	}

	@Override
	public boolean hasGeometryColumn() {
		return m_info.getGeometryColumnInfo().isPresent();
	}

	@Override
	public GeometryColumnInfo getGeometryColumnInfo() {
		return m_info.getGeometryColumnInfo()
						.getOrThrow(NoGeometryColumnException::new);
	}

	@Override
	public Envelope getBounds() {
		if ( hasGeometryColumn() ) {
			return new Envelope(m_info.getBounds());
		}
		else {
			throw new NoGeometryColumnException();
		}
	}

	@Override
	public String getHdfsPath() {
		return m_info.getFilePath();
	}

	@Override
	public long getBlockSize() {
		return m_info.getBlockSize();
	}

	@Override
	public long getRecordCount() {
		return m_info.getRecordCount();
	}

	@Override
	public FOption<String> getCompressionCodecName() {
		return m_info.getCompressionCodecName();
	}

	@Override
	public long length() {
		try {
			return HdfsPath.of(m_marmot.getHadoopFileSystem(), new Path(getHdfsPath()))
							.walkRegularFileTree()
							.map(UncheckedFunction.sneakyThrow(HdfsPath::getFileStatus))
							.mapToLong(FileStatus::getLen)
							.sum();
		}
		catch ( MarmotFileNotFoundException e ) {
			// DataSet가 catalog에만 등록되고, 실제 레코드가 하나도 삽입되지 않으면,
			// HDFS 파일이 생성되지 않을 수 있기 때문에, 이때는 0L을 반환한다.
			return 0L;
		}
		catch ( Exception e ) {
			throw new DataSetException(m_info.getId(), Throwables.unwrapThrowable(e));
		}
	}

	@Override
	public DataSet updateGeometryColumnInfo(FOption<GeometryColumnInfo> gcInfo) {
		m_info.setGeometryColumnInfo(gcInfo);
		m_marmot.getCatalog().insertOrReplaceDataSetInfo(m_info);
		
		return this;
	}

	@Override
	public RecordSet read() {
		return LoadDataSet.load(m_marmot, m_info.getId());
	}

	@Override
	public long append(RecordSet rset) {
		return append(rset, FOption.empty());
	}

	@Override
	public long append(RecordSet rset, String partId) {
		return append(rset, FOption.of(partId));
	}

	private long append(RecordSet rset, FOption<String> partId) {
		StoreDataSetOptions opts = partId.map(StoreDataSetOptions::APPEND)
											.getOrElse(StoreDataSetOptions.APPEND)
											.blockSize(m_info.getBlockSize());
		opts = m_info.getCompressionCodecName().transform(opts, (o,n) -> o.compressionCodecName(n));
		
		StoreDataSet store = new StoreDataSet(m_info.getId(), opts);
		store.initialize(m_marmot, rset.getRecordSchema());
		
		try ( CountingRecordSet countingRSet = rset.asCountingRecordSet() ) {
			store.consume(countingRSet);
			m_info = m_marmot.getDataSetInfo(m_info.getId());
			
			return countingRSet.getCount();
		}
	}
	
	@Override
	public boolean isSpatiallyClustered() {
		switch ( getType() ) {
			case FILE:
				return hasSpatialIndex();
			case SPATIAL_CLUSTER:
				return true;
			default:
				return false;
		}
	}

	@Override
	public Set<String> getClusterQuadKeyAll() throws NotSpatiallyClusteredException {
		Configuration conf = m_marmot.getHadoopConfiguration();
		
		if ( getType() == DataSetType.SPATIAL_CLUSTER ) {
			HdfsPath path = HdfsPath.of(conf, new Path(getHdfsPath()));
			SpatialClusterFile scFile = SpatialClusterFile.of(path);
			
			return scFile.getClusterKeyAll();
		}
		
		if ( !hasSpatialIndex() ) {
			throw new NotSpatiallyClusteredException("dataset id=" + getId());
		}
		
		return FStream.from(getSpatialIndexFile().getGlobalIndex().getIndexEntryAll())
								.map(GlobalIndexEntry::quadKey)
								.toSet();
	}
	
	@Override
	public boolean hasSpatialIndex() {
		if ( !hasGeometryColumn() ) {
			return false;
		}
		
		return m_catalog.getSpatialIndexCatalogInfo(getId()).isPresent();
	}
	
	public SpatialIndexedFile getSpatialIndexFile() {
		if ( !hasSpatialIndex() ) {
			throw IndexNotFoundException.fromDataSet(getId());
		}

		HdfsPath path = HdfsPath.of(m_marmot.getHadoopConfiguration(),
									m_catalog.generateSpatialIndexPath(getId(), getGeometryColumn()));
		return SpatialIndexedFile.load(path);
	}
	
	@Override
	public FOption<SpatialIndexInfo> getSpatialIndexInfo() {
		if ( !hasSpatialIndex() ) {
			return FOption.empty();
		}
		SpatialIndexedFile idxFile = getSpatialIndexFile();

		SpatialIndexInfo idxInfo = new SpatialIndexInfo(getId(), getGeometryColumnInfo());
		idxInfo.setHdfsFilePath(idxFile.getClusterDir().toString());
		String srcSrid = idxInfo.getGeometryColumnInfo().srid();
		if ( srcSrid.equals("EPSG:4326") ) {
			idxInfo.setTileBounds(idxFile.getQuadBounds());
			idxInfo.setDataBounds(idxFile.getDataBounds());
		}
		else {
			CoordinateTransform trans = CoordinateTransform.get("EPSG:4326", srcSrid);
			idxInfo.setTileBounds(trans.transform(idxFile.getQuadBounds()));
			idxInfo.setDataBounds(trans.transform(idxFile.getDataBounds()));
		}
		idxInfo.setRecordCount(idxFile.getRecordCount());
		idxInfo.setClusterCount(idxFile.getClusterCount());
		idxInfo.setNonDuplicatedRecordCount(idxFile.getOwnedRecordCount());
		
		return FOption.of(idxInfo);
	}

	@Override
	public SpatialIndexInfo createSpatialIndex(CreateSpatialIndexOptions opts) {
		Utilities.checkNotNullArgument(opts, "CreateSpatialIndexOptions should not be null");
		
		CreateSpatialIndexParameters params = new CreateSpatialIndexParameters();
		params.inputDataset(getId());
		params.sampleSize(opts.sampleSize());
		params.blockSize(opts.blockSize());
		params.workerCount(opts.workerCount());
		
		CreateSpatialIndex cluster = new CreateSpatialIndex();
		cluster.initialize(m_marmot, params.toMap());
		cluster.run();
		
		return getSpatialIndexInfo().get();
	}

	@Override
	public void deleteSpatialIndex() {
		m_catalog.getSpatialIndexCatalogInfo(getId())
				.ifPresent(catIdx -> {
					Path idxFilePath = new Path(catIdx.getHdfsFilePath());
					
					Catalog catalog = m_marmot.getCatalog();
					catalog.deleteSpatialIndexCatalogInfo(getId(), getGeometryColumn());
					m_marmot.getFileServer().deleteFileUpward(idxFilePath);
				});
	}

	@Override
	public Set<String> estimateQuadKeys(EstimateQuadKeysOptions opts) {
		LoadOptions loadOpts = opts.mapperCount()
									.map(cnt -> cnt > 0 ? LoadOptions.MAPPERS(cnt)
														: LoadOptions.FIXED_MAPPERS)
									.getOrElse(LoadOptions.DEFAULT);
		GeometryColumnInfo gcInfo = getGeometryColumnInfo();
		long clusterSize = opts.clusterSize().getOrElse(m_marmot.getDefaultClusterSize());
		long sampleSize = opts.sampleSize().getOrElse(m_marmot.getDefaultClusterSize());
		double sampleRatio = Math.min((double)sampleSize / length(), 1);
		long splitSize = Math.round(clusterSize * sampleRatio);
		String prjExpr = String.format("%s", SplitQuadSpace.COLUMN_QUADKEY);
		
		if ( s_logger.isInfoEnabled() ) {
			String ratioStr = String.format("%.2f%%", sampleRatio*100);
			String sampleSzStr = UnitUtils.toByteSizeString(sampleSize);
			String clusterSzStr = UnitUtils.toByteSizeString(clusterSize);
			String splitSzStr = UnitUtils.toByteSizeString(splitSize);
			s_logger.info("estimate quadkeys: sampleRatio={}, sampleSize={}, clusterSize={}, splitSize={}",
							ratioStr, sampleSzStr, clusterSzStr, splitSzStr);
		}

		Plan plan;
		plan = Plan.builder("estimate_quadkeys_" + getId())
					.load(getId(), loadOpts)
					.sample(sampleRatio)
					.add(new MBRTaggedOpaqueTransform(gcInfo, opts.validRange()))
					.reduce(new SplitQuadSpace(gcInfo, splitSize))
					.project(prjExpr)
					.shard(1)
					.build();
		return m_marmot.executeToRecordSet(plan).fstream()
						.map(r -> r.getString(0))
						.toSet();
	}

	@Override
	public void cluster(String outDsId, ClusterSpatiallyOptions opts) {
		Iterable<String> qkList;
		if ( opts.quadKeyList().isPresent() ) {
			qkList = opts.quadKeyList().getUnchecked();
		}
		else if ( opts.quadKeyDsId().isPresent() ) {
			DataSetImpl qkDs = m_marmot.getDataSet( opts.quadKeyDsId().getUnchecked());
			qkList = qkDs.getClusterQuadKeyAll();
		}
		else {
			long blockSize = opts.blockSize()
								.getOrElse(m_marmot.getDefaultBlockSize(DataSetType.SPATIAL_CLUSTER));
			long sampleSize = opts.sampleSize().getOrElse(blockSize);
			long clusterSize = opts.clusterSize().getOrElse(blockSize);
			
			EstimateQuadKeysOptions estOpts = EstimateQuadKeysOptions.DEFAULT()
																	.mapperCount(opts.mapperCount())
																	.sampleSize(sampleSize)
																	.clusterSize(clusterSize);
			qkList = estimateQuadKeys(estOpts);
		}
		
		String tmpDsId = (outDsId == null) ? getId() + "_C" : outDsId;
		try {
			LoadOptions loadOpts = opts.mapperCount()
										.map(cnt -> cnt > 0 ? LoadOptions.MAPPERS(cnt)
															: LoadOptions.FIXED_MAPPERS)
										.getOrElse(LoadOptions.DEFAULT);
			GeometryColumnInfo gcInfo = getGeometryColumnInfo();
			CreateDataSetOptions createOpts = CreateDataSetOptions.GEOMETRY(gcInfo)
															.force(opts.force())
															.type(DataSetType.SPATIAL_CLUSTER)
															.blockSize(opts.blockSize());
			m_marmot.createDataSet(tmpDsId, getRecordSchema(), createOpts);
			
			Plan plan;
			plan = Plan.builder("cluster_dataset_" + getId())
						.load(getId(), loadOpts)
						.clusterSpatially(tmpDsId, gcInfo, qkList, opts)
						.build();
			m_marmot.execute(plan);
			
			if ( outDsId == null ) {
				m_marmot.deleteDataSet(getId());
				m_marmot.moveDataSet(tmpDsId, getId());
				m_info = m_catalog.getDataSetInfo(getId()).get();
			}
		}
		finally {
			if ( outDsId == null ) {
				m_marmot.deleteDataSet(tmpDsId);
			}
		}
	}
	
	@Override
	public RangeQueryEstimate estimateRangeQuery(Envelope range) {
		FOption<CoordinateTransform> toWgs84 = FOption.ofNullable(getCoordinateTransform());
		FOption<CoordinateTransform> fromWgs84 = FOption.ofNullable(m_rtrans);
		Envelope rangeWgs84 = toWgs84.transform(range, (r,t) -> t.transform(r));

		QuadClusterFile<? extends QuadCluster> idxFile = loadOrBuildQuadClusterFile(m_marmot);
		List<ClusterEstimate> clusterEstimates
			= idxFile.queryClusters(rangeWgs84)
						.map(cluster -> {
							Envelope dataBounds = fromWgs84.transform(cluster.getDataBounds(), (b,t) -> t.transform(b));
							Envelope quadBounds = fromWgs84.transform(cluster.getQuadBounds(), (b,t) -> t.transform(b));
							
							Envelope clusterArea = quadBounds.intersection(dataBounds);
							Envelope matchingArea = range.intersection(clusterArea);
							double overlapRatio = matchingArea.getArea() / clusterArea.getArea();
							int matchCount = (int)Math.round(cluster.getOwnedRecordCount() * overlapRatio);
							
							return new ClusterEstimate(cluster.getQuadKey(), (int)cluster.getRecordCount(),
														matchCount);
						})
						.toList();
		
		return new RangeQueryEstimate(getId(), range, clusterEstimates);
	}

	@Override
	public RecordSet queryRange(Envelope range, int nsamples) {
		Utilities.checkNotNullArgument(range, "range is null");
		Utilities.checkArgument(nsamples > 0, "nsamples > 0");
		
		RangeQuery query = new RangeQuery(m_marmot, this, range, nsamples);
		return query.run();
	}
	
	@Override
	public RecordSet readSpatialCluster(String quadKey) {
		Utilities.checkNotNullArgument(quadKey, "quad-key is null");
		
		SpatialIndexedCluster cluster = getSpatialIndexFile().getCluster(quadKey);
		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug("read cluster: {}", cluster);
		}
		return RecordSet.from(cluster.getRecordSchema(),
							cluster.read(false).map(EnvelopeTaggedRecord::getRecord));
	}

	public RecordSet readThumbnail(Envelope bounds, int sampleCount)
		throws ThumbnailNotFoundException, InsufficientThumbnailException, IOException {
		if ( !hasThumbnail() ) {
			throw new ThumbnailNotFoundException(getId());
		}
		
		RecordSet rset = new ThumbnailRecordSet(this, bounds, sampleCount);
		
		Project prj = new Project("*-{__quadKey}");
		prj.initialize(m_marmot, rset.getRecordSchema());
		return prj.apply(rset);
	}

	@Override
	public void createThumbnail(int sampleCount) throws IndexNotFoundException {
		if ( !hasSpatialIndex() ) {
			throw IndexNotFoundException.fromDataSet(getId());
		}
		Utilities.checkArgument(sampleCount > 1000,
							"sample count is too small: nsmaples=" + sampleCount);
		if ( sampleCount >= Math.round(getRecordCount()*.7f) ) {
			String msg = String.format("sample count is too large to the dataset: "
									+ "nsamples=%d, dataset nrecords=%d", sampleCount, getRecordCount());
			throw new IllegalArgumentException(msg);
		}
		
		HdfsPath thumbnaialPath = getThumbnailPath();
		thumbnaialPath.delete();
		
		long total = getSpatialIndexFile().getOwnedRecordCount();
		double ratio = (double)sampleCount / total;
		
		CreateThumbnail create = new CreateThumbnail(getGeometryColumn(), ratio);
		Plan plan = Plan.builder("create thumbnail")
							.loadSpatialClusteredFile(getId(),
											"quad_key as __quadKey, owned_count as __count")
							.add(create)
							.listByGroup(Group.ofKeys("__quadKey").workerCount(1))
							.project("*-{__quadKey}, __quadKey")
							.storeMarmotFile(thumbnaialPath.toString())
							.build();
		m_marmot.execute(plan);
		
		m_info.setThumbnailRatio((float)ratio);

		Catalog catalog = m_marmot.getCatalog();
		catalog.insertOrReplaceDataSetInfo(m_info);
	}

	@Override
	public boolean deleteThumbnail() {
		HdfsPath path = getThumbnailPath();
		if ( path.exists() ) {
			return path.delete();
		}
		else {
			return false;
		}
	}

	@Override
	public FOption<Float> getThumbnailRatio() {
		float ratio = m_info.getThumbnailRatio();
		if ( ratio < 0 ) {
			m_info = m_marmot.getDataSetInfo(m_info.getId());
			ratio = m_info.getThumbnailRatio();
		}
		if ( ratio >= 0 ) {
			return FOption.of(ratio);
		}
		
		return FOption.empty();
	}
	
	public HdfsPath getThumbnailPath() {
		return HdfsPath.of(m_marmot.getHadoopFileSystem(), new Path(THUMBNAIL_PREFIX + getId()));
	}

	public boolean hasThumbnail() {
		return getThumbnailPath().exists();
	}
	
	public Envelope toWgs84(Envelope range) {
		FOption<CoordinateTransform> toWgs84 = FOption.ofNullable(getCoordinateTransform());
		return toWgs84.transform(range, (r,t) -> t.transform(r));
	}
	
	/**
	 * 주어진 데이터세트의 공간 클러스터 파일을 반환한다.
	 * <p>
	 * 만일 주어진 데이터세트에 해당하는 공간 클러스터 파일이 존재하지 않는 경우,
	 * 대상 데이터세트의 크기가 디스크 블럭보다 작은 경우에는 즉석에서 생성하여
	 * 반환한다. 그렇지 않은 경우는 예외를 발생시킴
	 * 
	 * @param ds	공간 데이터세트
	 * @return	공간 클러스터 인덱스.
	 * @throws IndexNotFoundException	대상 데이터세트에 공간 색인이 존재하지 않는 경우.
	 */
	public QuadClusterFile<? extends CacheableQuadCluster>
	loadOrBuildQuadClusterFile(MarmotCore marmot) throws IndexNotFoundException {
		if ( hasSpatialIndex() ) {
			return getSpatialIndexFile();
		}
		if ( getType().equals(DataSetType.SPATIAL_CLUSTER) ) {
			HdfsPath path = HdfsPath.of(m_marmot.getHadoopFileSystem(), new Path(getHdfsPath()));
			return SpatialClusterFile.of(path);
		}

		// lookup-table dataset가 heap인 경우는 그 크기가 cluster 최대 크기보다 작은 경우는
		// 즉석해서 clustering해서 사용하고, 그렇지 않은 경우는 예외를 발생시킨다.
		long size = length();
		if ( size > marmot.getDefaultClusterSize() ) {
			throw IndexNotFoundException.fromDataSet(getId());
		}
		
		return new InMemoryIndexedClusterBuilder(getGeometryColumnInfo(), read(), 8).get();
	}
	
	@Override
	public String toString() {
		return m_info.toString();
	}
	
	private CoordinateTransform getCoordinateTransform() {
		if ( m_trans == null ) {
			String srid = getGeometryColumnInfo().srid();
			m_trans = CoordinateTransform.get(srid, "EPSG:4326");
			m_rtrans = m_trans.inverse();
		}
		
		return m_trans;
	}
}
