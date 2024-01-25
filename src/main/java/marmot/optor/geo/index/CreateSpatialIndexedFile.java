package marmot.optor.geo.index;

import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;

import com.google.common.collect.Maps;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.dataset.GeometryColumnInfo;
import marmot.io.MarmotFileWriteOptions;
import marmot.io.geo.index.GlobalIndex;
import marmot.optor.CompositeRecordSetConsumer;
import marmot.optor.StoreAsHeapfile;
import marmot.support.RecordSetOperatorChain;
import utils.UnitUtils;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CreateSpatialIndexedFile extends CompositeRecordSetConsumer {
	private final Path m_clusterDir;
	private final Set<String> m_qkeys;
	private final GeometryColumnInfo m_geomCol;
	private final long m_blockSize;
	private final int m_workerCount;
	
	public CreateSpatialIndexedFile(Path clusterDir, GeometryColumnInfo geomCol, Set<String> qkeys,
									long blockSize, int workerCount) {
		Utilities.checkNotNullArgument(clusterDir, "clusterDir is null");
		Utilities.checkNotNullArgument(qkeys, "qkSource is null");
		Utilities.checkNotNullArgument(geomCol, "geomCol is null");
		Utilities.checkArgument(blockSize > 0, "invalid block-size: " + blockSize);
		Utilities.checkArgument(workerCount > 0, "invalid worker-count: " + workerCount);

		m_clusterDir = clusterDir;
		m_qkeys = qkeys;
		m_geomCol = geomCol;
		m_blockSize = blockSize;
		m_workerCount = workerCount;
	}

	@Override
	protected void _initialize(MarmotCore marmot, RecordSchema inputSchema) { }

	@Override
	protected RecordSetOperatorChain createComponents() {
		RecordSetOperatorChain components = RecordSetOperatorChain.from(m_marmot, m_inputSchema);
		
		CreateIndexedClusters create = new CreateIndexedClusters(m_geomCol, m_qkeys, m_clusterDir,
																m_blockSize, m_workerCount);
		components.add(create);
		
		// 여기서 생성된 SpatialClusterIndexFile은 reducer의 갯수에 따라 여러 파일로
		// 생성될 수 있다. 이들은 이후 수행될 'CreateGlobalIndex' 연산에 의해
		// 다시 하나의 단일 파일로 생성되게 된다.
		Path clusterIndexFile = GlobalIndex.toGlobalIndexPath(m_clusterDir);
		Map<String,String> metaData = Maps.newHashMap();
		metaData.put(GlobalIndex.PROP_KEY_CLUSTER_SCHEMA, m_inputSchema.toString());
		components.add(new StoreAsHeapfile(clusterIndexFile, MarmotFileWriteOptions.META_DATA(metaData)));

		return components;
	}
	
	@Override
	public String toString() {
		return String.format("%s: dir=%s, block_size=%s", getClass().getSimpleName(),
								m_clusterDir, UnitUtils.toByteSizeString(m_workerCount));
	}
}
