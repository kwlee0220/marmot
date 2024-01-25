package marmot.optor.join;

import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.support.colexpr.ColumnSelector;
import marmot.rset.AbstractRecordSet;
import marmot.support.DefaultRecord;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class PartitionInnerJoin extends AbstractRecordSet {
	private final RecordSet m_rset;
	private final List<Record> m_cache;
	private final int m_cachingDsIdx;
	
	private final ColumnSelector m_combiner;
	private final Map<String,Record> m_binding = Maps.newHashMap();
	private int m_cacheIdx;
	private Record m_inputRecord;
	
	PartitionInnerJoin(RecordSet nonCacheds, List<Record> cacheds,
						int cachingDsIdx, ColumnSelector combiner) {
		Utilities.checkNotNullArgument(nonCacheds, "nonCacheds is null");
		Utilities.checkNotNullArgument(cacheds, "cacheds is null");
		Preconditions.checkArgument(cachingDsIdx == 0 || cachingDsIdx == 1);
		Utilities.checkNotNullArgument(combiner, "combiner is null");
		
		m_rset = nonCacheds;
		m_cache = cacheds;
		m_cachingDsIdx = cachingDsIdx;
		m_combiner = combiner;
		
		m_cacheIdx = m_cache.size();
		m_inputRecord = DefaultRecord.of(nonCacheds.getRecordSchema());
	}
	
	@Override protected void closeInGuard() { }

	@Override
	public RecordSchema getRecordSchema() {
		return m_combiner.getRecordSchema();
	}

	@Override
	public boolean next(Record output) {
		if ( m_cacheIdx >= m_cache.size() ) {
			if ( !m_rset.next(m_inputRecord) ) {
				return false;
			}
			
			m_binding.put((m_cachingDsIdx == 1) ? "left" : "right", m_inputRecord);
			m_cacheIdx = 0;
		}
		
		Record cachedRecord = m_cache.get(m_cacheIdx++);
		m_binding.put((m_cachingDsIdx == 0) ? "left" : "right", cachedRecord);
		m_combiner.select(m_binding, output);
		
		return true;
	}
}
