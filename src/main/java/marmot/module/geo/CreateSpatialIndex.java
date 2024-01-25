package marmot.module.geo;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.MarmotCore;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.analysis.module.geo.CreateSpatialIndexParameters;
import marmot.dataset.Catalog;
import marmot.dataset.DataSet;
import marmot.dataset.DataSetImpl;
import marmot.dataset.DataSetType;
import marmot.dataset.GeometryColumnInfo;
import marmot.dataset.SpatialIndexCatalogInfo;
import marmot.geo.catalog.SpatialIndexInfo;
import marmot.geo.command.EstimateQuadKeysOptions;
import marmot.io.HdfsPath;
import marmot.io.geo.cluster.SpatialClusterFile;
import marmot.io.geo.index.GlobalIndex;
import marmot.io.geo.index.GlobalIndexEntry;
import marmot.module.MarmotModule;
import marmot.optor.LoadDataSet;
import marmot.optor.LoadMarmotFile;
import marmot.optor.geo.cluster.SampledQuadSpace;
import marmot.optor.geo.index.CreateSpatialIndexedFile;
import marmot.plan.LoadOptions;
import marmot.support.RecordSetOperatorChain;
import marmot.type.DataType;
import utils.UnitUtils;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CreateSpatialIndex implements MarmotModule {
	private static final Logger s_logger = LoggerFactory.getLogger(DataSetImpl.class);
	
	private static final RecordSchema SCHEMA
							= RecordSchema.builder()
										.addColumn(SampledQuadSpace.COLUMN_QUADKEY, DataType.STRING)
										.build();

	private MarmotCore m_marmot;
	private CreateSpatialIndexParameters m_params;

	@Override
	public List<String> getParameterNameAll() {
		return CreateSpatialIndexParameters.getParameterNameAll();
	}
	
	@Override
	public RecordSchema getOutputRecordSchema(MarmotCore marmot, Map<String, String> paramsMap) {
		return SCHEMA;
	}

	@Override
	public void initialize(MarmotCore marmot, Map<String, String> paramsMap) {
		m_marmot = marmot;
		m_params = CreateSpatialIndexParameters.fromMap(paramsMap);
	}

	@Override
	public void run() {
		String input = m_params.inputDataset();
		
		DataSetImpl ds = m_marmot.getDataSetOrNull(input);
		if ( ds == null ) {
			throw new IllegalArgumentException("invalid dataset id: id=" + input);
		}
		if ( !ds.hasGeometryColumn() ) {
			throw new IllegalArgumentException("input dataset does not have the default Geometry "
												+ "column: dataset=" + input);
		}

		long blockSize = m_params.blockSize().getOrElse(() -> m_marmot.getDefaultMarmotBlockSize());
		Set<String> quadKeyList = m_params.quadKeyDataSet()
										.map(this::loadQuadKeyList)
										.getOrElse(() -> estimateQuadKeys(ds, blockSize));
		createSpatialIndexedFile(ds, quadKeyList, blockSize);
		
		SpatialIndexInfo idxInfo = ds.getSpatialIndexInfo().get();
		s_logger.info("clustered: nclusters={} nrecords={}, non-duplicated={}",
						idxInfo.getClusterCount(), idxInfo.getRecordCount(),
						idxInfo.getNonDuplicatedRecordCount());
	}
	
	@Override
	public String toString() {
		return String.format("%s[input=%s%s]", getClass().getSimpleName(),
								m_params.inputDataset(),
								m_params.blockSize()
										.map(size -> String.format(", block_size=%s",
												UnitUtils.toByteSizeString(size)))
										.getOrElse(""));
	}
	
	private Set<String> estimateQuadKeys(DataSet ds, long blockSize) {
		long sampleSize = m_params.sampleSize().getOrElse(blockSize);
		EstimateQuadKeysOptions opts = EstimateQuadKeysOptions.SAMPLE_SIZE(sampleSize);
		
		return ds.estimateQuadKeys(opts);
	}
	
	private void createSpatialIndexedFile(DataSetImpl ds, Set<String> quadKeys, long blockSize) {
		ds.deleteSpatialIndex();

		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		Catalog catalog = m_marmot.getCatalog();
		Path dir = catalog.generateSpatialIndexPath(ds.getId(), gcInfo.name());
		int workerCount = m_params.workerCount().getOrElse(m_marmot.getDefaultPartitionCount());

		m_marmot.getFileServer().deleteFileUpward(dir);
		
		RecordSetOperatorChain chain = RecordSetOperatorChain.from(m_marmot);
		chain.add(new LoadDataSet(ds.getId(), LoadOptions.DEFAULT));
		chain.add(new CreateSpatialIndexedFile(dir, gcInfo, quadKeys, blockSize, workerCount));
		m_marmot.execute("create_spatial_index", chain);
		
		// 여러 reducer에 의해 global index가 생성되면 global index가 하나의 파일이 아니라
		// 디렉토리로 구성되기 때문에, LoadMarmotFile을 써서 전체를 읽어서 다시
		// 단일 파일로 저장한다.
		HdfsPath globalIdxPath = HdfsPath.of(m_marmot.getHadoopConfiguration(), 
												GlobalIndex.toGlobalIndexPath(dir));
		List<GlobalIndexEntry> entries = LoadMarmotFile.load(m_marmot, globalIdxPath.toString())
														.fstream()
														.map(GlobalIndexEntry::from)
														.toList();
		globalIdxPath.delete();
		GlobalIndex.create(globalIdxPath, ds.getGRecordSchema(), entries);
		
		// 카다로그에 생성된 인덱스 등록정보를 저장함.
		SpatialIndexCatalogInfo catInfo = new SpatialIndexCatalogInfo(ds.getId(), gcInfo, dir.toString());
		catalog.insertSpatialIndexCatalogInfo(catInfo);
	}
	
	private Set<String> loadQuadKeyList(String quadKeyDsId) {
		DataSetImpl qkeyDs = m_marmot.getDataSet(quadKeyDsId);
		if ( qkeyDs.getType() == DataSetType.SPATIAL_CLUSTER ) {
			Configuration conf = qkeyDs.getMarmotCore().getHadoopConfiguration();
			HdfsPath path = HdfsPath.of(conf, new Path(qkeyDs.getHdfsPath()));
			
			SpatialClusterFile scFile = SpatialClusterFile.of(path);
			return scFile.getClusterKeyAll();
		}
		
		if ( qkeyDs.getSpatialIndexInfo().isAbsent() ) {
			throw new IllegalArgumentException("dataset does not have quadkeys: ds=" + quadKeyDsId);
		}

		Plan plan;
		plan = Plan.builder("list_spatial_clusters")
					.loadSpatialClusterIndexFile(quadKeyDsId)
					.project("quad_key")
					.build();
		return m_marmot.executeLocally(plan).fstream()
						.map(rec -> rec.getString(0))
						.toSet();
	}
}
