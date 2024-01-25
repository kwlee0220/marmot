package marmot.optor.geo.cluster;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.dataset.DataSetType;
import marmot.dataset.GeometryColumnInfo;
import marmot.io.HdfsPath;
import marmot.io.MultiColumnKey;
import marmot.io.geo.cluster.SpatialClusterFile;
import marmot.io.geo.cluster.SpatialClusterInfo;
import marmot.mapreduce.MarmotMRContexts;
import marmot.mapreduce.MarmotMapOutputKeyColumns;
import marmot.mapreduce.ReduceContextKeyedRecordSetFactory;
import marmot.mapreduce.ReduceContextRecordSet;
import marmot.optor.MapReduceJoint;
import marmot.optor.StoreDataSetOptions;
import marmot.optor.rset.InMemoryKeyedRecordSetFactory;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.optor.support.KeyedRecordSetFactory;
import marmot.proto.optor.StoreSpatialClusterPackProto;
import marmot.rset.AbstractRecordSet;
import marmot.support.PBSerializable;
import marmot.support.ProgressReportable;
import utils.StopWatch;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StoreSpatialClusterPack extends AbstractRecordSetFunction
										implements PBSerializable<StoreSpatialClusterPackProto> {
	@SuppressWarnings("unused")
	private static final Logger s_logger = LoggerFactory.getLogger(StoreSpatialClusterPack.class);
	public static final RecordSchema SCHEMA = SpatialClusterInfo.SCHEMA;

	private final String m_outDsId;
	private final int m_partitionCount;
	private final StoreDataSetOptions m_opts;
	
	private Path m_clusterDir;	// set when initialized;
	
	public StoreSpatialClusterPack(String dsId, int partCount, StoreDataSetOptions opts) {
		m_outDsId = dsId;
		m_partitionCount = partCount;
		m_opts = opts;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		m_clusterDir = marmot.getCatalog().generateFilePath(m_outDsId);
		
		setInitialized(marmot, inputSchema, SCHEMA);
	}

	@Override
	public ClusterInfoCollecteds apply(RecordSet input) {
		checkInitialized();
		
		MultiColumnKey grpKey = MultiColumnKey.of(AttachQuadKeyRSet.COL_QUAD_KEY);
		
		KeyedRecordSetFactory groups;
		if ( input instanceof ReduceContextRecordSet ) {
			@SuppressWarnings("rawtypes")
			ReduceContext ctxt = ((ReduceContextRecordSet)input).getReduceContext();
			groups = new ReduceContextKeyedRecordSetFactory(ctxt, input.getRecordSchema(),
															grpKey, MultiColumnKey.EMPTY);
		}
		else {
			groups = new InMemoryKeyedRecordSetFactory(input, grpKey, false);
		}
		
		long blkSize = m_marmot.getDefaultBlockSize(DataSetType.SPATIAL_CLUSTER);
		return new ClusterInfoCollecteds(this, groups, m_opts.blockSize().getOrElse(blkSize));
	}

	@Override
	public MapReduceJoint getMapReduceJoint(RecordSchema inputSchema) {
		checkInitialized();
		
		MarmotCore marmot = (MarmotCore)m_marmot;
		MultiColumnKey clusterKeyCols = MultiColumnKey.fromString(AttachQuadKeyRSet.COL_QUAD_KEY) ;
		MarmotMapOutputKeyColumns mokCols = MarmotMapOutputKeyColumns.fromGroupKey(clusterKeyCols);
		
		MapReduceJoint joint = MapReduceJoint.create()
								.setMapOutputKey(mokCols)
								.setReducerCount(m_partitionCount)
								.addReducer(this);
		if ( joint.getReducerCount() > 1 ) {
			joint.setPartitionerClass(HashPartitioner.class);
		}
		
		return joint;
	}
	
	@Override
	public String toString() {
		return String.format("%s: path=%s, %s", getClass().getSimpleName(), m_clusterDir,
								m_opts.toOptionsString());
	}
	
	public static StoreSpatialClusterPack fromProto(StoreSpatialClusterPackProto proto) {
		return new StoreSpatialClusterPack(proto.getDsId(), proto.getPartitionCount(),
											StoreDataSetOptions.fromProto(proto.getOptions()));
	}

	@Override
	public StoreSpatialClusterPackProto toProto() {
		return StoreSpatialClusterPackProto.newBuilder()
											.setDsId(m_outDsId)
											.setPartitionCount(m_partitionCount)
											.setOptions(m_opts.toProto())
											.build();
	}

	private static class ClusterInfoCollecteds extends AbstractRecordSet implements ProgressReportable {
		private final StoreSpatialClusterPack m_store;
		private final GeometryColumnInfo m_gcInfo;
		private final KeyedRecordSetFactory m_grsFact;
		private final HdfsPath m_packPath;
		private final long m_blockSize;

		ClusterInfoCollecteds(StoreSpatialClusterPack store, KeyedRecordSetFactory grsFact,
								long blockSize) {
			m_store = store;
			m_grsFact = grsFact;
			
			String partId = String.format("%05d", MarmotMRContexts.getTaskOrdinal().getOrElse(0));
			m_packPath = HdfsPath.of(store.m_marmot.getHadoopConfiguration(), store.m_clusterDir)
									.child(partId);

			m_gcInfo = m_store.m_opts.geometryColumnInfo().get();
			m_blockSize = blockSize;
		}

		@Override
		protected void closeInGuard() throws Exception {
			m_grsFact.close();
		}

		@Override
		public RecordSchema getRecordSchema() {
			return SCHEMA;
		}
		
		@Override
		public Record nextCopy() {
			return m_grsFact.nextKeyedRecordSet()
							.map(krset -> {
								String quadKey = (String)krset.getKey().getValueAt(0);
								
								SpatialClusterInfo info
										= SpatialClusterFile.storeCluster(m_packPath, quadKey,
																	m_gcInfo, krset, m_blockSize);
								return info.toRecord();
							})
							.getOrNull();
		}
		
		@Override
		public void reportProgress(Logger logger, StopWatch elapsed) {
			m_grsFact.reportProgress(logger, elapsed);
			logger.info("report: {}", m_store.toString());
		}
	}
}
