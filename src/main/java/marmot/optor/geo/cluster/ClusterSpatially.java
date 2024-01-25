package marmot.optor.geo.cluster;

import java.util.Map;

import org.apache.hadoop.fs.Path;

import com.google.common.collect.Maps;

import marmot.MarmotCore;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.command.ClusterSpatiallyOptions;
import marmot.io.MarmotFileWriteOptions;
import marmot.io.geo.cluster.SpatialClusterFile;
import marmot.optor.CompositeRecordSetConsumer;
import marmot.optor.StoreDataSetOptions;
import marmot.optor.UpdateDataSetInfo;
import marmot.proto.optor.ClusterSpatiallyProto;
import marmot.support.PBSerializable;
import marmot.support.RecordSetOperatorChain;
import utils.CSV;
import utils.func.Either;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ClusterSpatially extends CompositeRecordSetConsumer
								implements PBSerializable<ClusterSpatiallyProto> {
	private final String m_outDsId;
	private final GeometryColumnInfo m_gcInfo;
	private final Either<String, String> m_quadKeyInfo;
	private final ClusterSpatiallyOptions m_options;
	
	private int m_partCount;
	private Iterable<String> m_quadKeyList;
	
	public ClusterSpatially(String outDsId, GeometryColumnInfo gcInfo,
							Either<String, String> quadKeyInfo, ClusterSpatiallyOptions opts) {
		m_outDsId = outDsId;
		m_gcInfo = gcInfo;
		m_quadKeyInfo = quadKeyInfo;
		m_options = opts;
	}

	@Override
	protected void _initialize(MarmotCore marmot, RecordSchema inputSchema) {
		if ( m_quadKeyInfo.left().isPresent() ) {
			m_quadKeyList = CSV.parseCsv(m_quadKeyInfo.left().getUnchecked()).toList();
		}
		else {
			m_quadKeyList = marmot.getDataSet(m_quadKeyInfo.right().getUnchecked())
									.getClusterQuadKeyAll();
		}
		m_partCount = m_options.partitionCount().getOrElse(marmot.getDefaultPartitionCount());
	}

	@Override
	protected RecordSetOperatorChain createComponents() {
		StoreDataSetOptions storeOpts = StoreDataSetOptions.FORCE(m_options.force())
														.geometryColumnInfo(m_gcInfo)
														.blockSize(m_options.blockSize());
		
		MarmotFileWriteOptions writeOpts = MarmotFileWriteOptions.FORCE(m_options.force())
													.blockSize(m_options.blockSize());
		Map<String,String> meta = writeOpts.metaData().getOrElse(Maps.newHashMap());
		meta.put(SpatialClusterFile.PROP_DATASET_SCHEMA, getRecordSchema().toString());
		meta.put(SpatialClusterFile.PROP_GEOM_COL, m_gcInfo.name());
		meta.put(SpatialClusterFile.PROP_SRID, m_gcInfo.srid());
		writeOpts = writeOpts.metaData(meta);

		String prjExpr = String.format("*-{%s}", AttachQuadKeyRSet.COL_MBR);
		
		Path clusterPath = m_marmot.getCatalog().generateFilePath(m_outDsId);
		String infoPath = new Path(clusterPath, SpatialClusterFile.CLUSTER_INDEX_FILE).toString();
		
		Plan plan;
		plan = Plan.builder("cluster_dataset")
					.attachQuadKey(m_gcInfo, m_quadKeyList, m_options.validRange(), true, false)
					.project(prjExpr)
					.add(new StoreSpatialClusterPack(m_outDsId, m_partCount, storeOpts))
					.shard(1)
					.tee(infoPath.toString(), writeOpts)
					.project("data_bounds as mbr,count,length as size")
					.add(new UpdateDataSetInfo(m_outDsId))
					.build();
		return RecordSetOperatorChain.from(m_marmot, plan);
	}

	@Override
	public String toString() {
		return String.format("%s: geom=%s, part_count=%d",
							getClass().getSimpleName(), m_gcInfo, m_partCount);
	}
	
	public static ClusterSpatially fromProto(ClusterSpatiallyProto proto) {
		GeometryColumnInfo gcInfo = GeometryColumnInfo.fromProto(proto.getGeometryColumnInfo());
		
		Either<String, String> qkInfo;
		switch ( proto.getEitherQuadKeyInfoCase() ) {
			case QUAD_KEY_LIST:
				qkInfo = Either.left(proto.getQuadKeyList());
				break;
			case QUAD_KEY_DS_ID:
				qkInfo = Either.right(proto.getQuadKeyDsId());
				break;
			default:
				throw new AssertionError();
		}
		ClusterSpatiallyOptions opts = ClusterSpatiallyOptions.fromProto(proto.getOptions());
		
		return new ClusterSpatially(proto.getOutDsId(), gcInfo, qkInfo, opts);
	}

	@Override
	public ClusterSpatiallyProto toProto() {
		ClusterSpatiallyProto.Builder builder = ClusterSpatiallyProto.newBuilder()
															.setOutDsId(m_outDsId)
															.setGeometryColumnInfo(m_gcInfo.toProto())
															.setOptions(m_options.toProto());
		builder = m_quadKeyInfo.left().transform(builder, (b,l) -> b.setQuadKeyList(l));
		builder = m_quadKeyInfo.right().transform(builder, (b,id) -> b.setQuadKeyDsId(id));
		return builder.build();
	}
}
