package marmot.optor.geo.index;

import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.io.MultiColumnKey;
import marmot.io.geo.index.GlobalIndexEntry;
import marmot.mapreduce.MarmotMapOutputKeyColumns;
import marmot.optor.MapReduceJoint;
import marmot.optor.RecordSetOperator;
import marmot.optor.geo.cluster.AttachQuadKey;
import marmot.optor.geo.cluster.AttachQuadKeyRSet;
import marmot.optor.geo.filter.DropEmptyGeometry;
import marmot.optor.geo.filter.SpatialFilters;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.optor.support.RecordSetOperators;
import utils.UnitUtils;
import utils.Utilities;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CreateIndexedClusters extends AbstractRecordSetFunction  {
	private static final MultiColumnKey KEY = MultiColumnKey.of(AttachQuadKeyRSet.COL_QUAD_KEY);

	private final GeometryColumnInfo m_gcInfo;
	private final Path m_clusterDir;
	private final long m_blockSize;
	private final int m_workerCount;

	// Geometry 정보가 없는 레코드는 cluster 생성에서 제외시키기 위한 필터 생성
	private final DropEmptyGeometry m_dropEmpty;
	private final AttachQuadKey m_attach;
	private final CreateIndexedClusterPack m_collect;
	
	public CreateIndexedClusters(GeometryColumnInfo gcInfo, Set<String> qkeys, Path clusterDir,
								long blockSize, int workerCount) {
		Utilities.checkNotNullArgument(gcInfo, "GeometryColumnInfo is null");
		Utilities.checkNotNullArgument(qkeys, "QuadKeys is null");
		Utilities.checkNotNullArgument(clusterDir, "clusterDir is null");
		Utilities.checkArgument(blockSize > 0, "invalid block-size: " + blockSize);
		Utilities.checkArgument(workerCount > 0, "invalid worker-count: " + workerCount);
		
		m_clusterDir = clusterDir;
		m_gcInfo = gcInfo;
		m_blockSize = blockSize;
		m_workerCount = workerCount;

		// Geometry 정보가 없는 레코드는 cluster 생성에서 제외시키기 위한 필터 생성
		m_dropEmpty = SpatialFilters.skipEmptyGeometry(gcInfo.name());
		
		m_attach = new AttachQuadKey(m_gcInfo, qkeys, FOption.empty(), true, false);
		m_collect = new CreateIndexedClusterPack(clusterDir, m_gcInfo, blockSize);
	}

	@Override
	public boolean isStreamable() {
		return false;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		m_attach.initialize(marmot, inputSchema);
		m_collect.initialize(marmot, inputSchema);
		
		setInitialized(marmot, inputSchema, GlobalIndexEntry.SCHEMA);
	}

	@Override
	public RecordSet apply(RecordSet input) {
		checkInitialized();
		
		List<RecordSetOperator> optors = Lists.newArrayList(m_attach, m_collect);
		return RecordSetOperators.run(m_marmot, optors, input);
	}

	@Override
	public MapReduceJoint getMapReduceJoint(RecordSchema inputSchema) {
		return MapReduceJoint.create()
						.setMapOutputKey(MarmotMapOutputKeyColumns.fromGroupKey(KEY))
						.setReducerCount(m_workerCount)
						.addMapper(m_dropEmpty, m_attach)
						.addReducer(m_collect);
	}
	
	@Override
	public String toString() {
		return String.format("%s: dir=%s, block-size=%s", getClass().getSimpleName(),
								m_clusterDir, UnitUtils.toByteSizeString(m_blockSize));
	}
}
