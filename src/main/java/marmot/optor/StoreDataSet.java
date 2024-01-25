package marmot.optor;

import java.util.UUID;

import org.apache.hadoop.fs.Path;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.dataset.DataSetImpl;
import marmot.proto.optor.StoreDataSetProto;
import marmot.support.PBSerializable;
import marmot.support.RecordSetOperatorChain;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StoreDataSet extends CompositeRecordSetConsumer
							implements PBSerializable<StoreDataSetProto> {
	private final String m_dsId;
	private final StoreDataSetOptions m_options;
	private DataSetImpl m_ds;	// set during initialization
	private boolean m_deleted = false;
	
	public StoreDataSet(String dsId, StoreDataSetOptions opts) {
		Utilities.checkNotNullArgument(dsId, "target dataset is null");
		Utilities.checkNotNullArgument(opts, "StoreDataSetOptions is null");
		Utilities.checkArgument(!(opts.append().getOrElse(false) && opts.force()),
								() -> "Both 'append' and 'force' should not be true");
		
		m_dsId = dsId;
		m_options = opts;
	}
	
	public String getDataSetId() {
		return m_dsId;
	}

	@Override
	protected void _initialize(MarmotCore marmot, RecordSchema inputSchema) {
		try {
			m_ds = marmot.getDataSetOrNull(m_dsId);
			
			// force가 true인 경우에는 무조건 대상 데이터세트를 먼저 삭제한다. 
			if ( !m_deleted && m_options.force() && m_ds != null ) {
				marmot.deleteDataSet(m_dsId);
				m_ds = null;
				m_deleted = true;
			}
			if ( !m_options.append().getOrElse(false) ) {
				m_ds = marmot.createDataSet(m_dsId, inputSchema, m_options.toCreateOptions());
			}
			else if ( m_ds == null ) {
				throw new IllegalArgumentException("target dataset for append does not exist: ds=" + m_dsId);
			}
		}
		catch ( Exception e ) {
			throw new RecordSetOperatorException("op=store: " +  e);
		}
	}
	
	@Override
	protected RecordSetOperatorChain createComponents() {
		checkInitialized();
		
		StoreDataSetOptions opts = m_options;
		if ( m_ds.hasGeometryColumn() ) {
			opts = opts.geometryColumnInfo(m_ds.getGeometryColumnInfo());
		}
		
		Path homeDir = new Path(m_ds.getHdfsPath());
		Path filePath;
		if ( opts.partitionId().isPresent() ) {
			filePath = new Path(homeDir, opts.partitionId().get());
		}
		else {
			filePath = new Path(homeDir, UUID.randomUUID().toString());
		}
		
		StoreDataSetPartition storePartitions = new StoreDataSetPartition(filePath, opts);
		UpdateDataSetInfo updateInfo = new UpdateDataSetInfo(m_dsId);
		
		return RecordSetOperatorChain.from(m_marmot, m_inputSchema)
									.addAll(storePartitions, updateInfo);
	}
	
	public static void store(MarmotCore marmot, String dsId, RecordSet rset, StoreDataSetOptions opts) {
		StoreDataSet store = new StoreDataSet(dsId, opts);
		store.initialize(marmot, rset.getRecordSchema());
		store.consume(rset);
	}
	
	@Override
	public String toString() {
		return String.format("StoreDataSet[%s,opts=%s]", m_dsId, m_options);
	}

	public static StoreDataSet fromProto(StoreDataSetProto proto) {
		StoreDataSetOptions opts = StoreDataSetOptions.fromProto(proto.getOptions());
		return new StoreDataSet(proto.getId(), opts);
	}

	@Override
	public StoreDataSetProto toProto() {
		return StoreDataSetProto.newBuilder()
								.setId(m_dsId)
								.setOptions(m_options.toProto())
								.build();
	}
}
