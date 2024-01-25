package marmot.optor.join;

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
class SingleSidePartitionJoin extends AbstractRecordSet {
	private final RecordSet m_rset;
	private final int m_dsIdx;
	
	private final ColumnSelector m_combiner;
	private final Map<String,Record> m_binding = Maps.newHashMap();
	private Record m_inputRecord;
	
	SingleSidePartitionJoin(RecordSchema leftSchema, RecordSchema rightSchema,
							RecordSet rset, int dsIdx, ColumnSelector combiner) {
		Utilities.checkNotNullArgument(rset, "rset is null");
		Preconditions.checkArgument(dsIdx == 0 || dsIdx == 1);
		Utilities.checkNotNullArgument(combiner, "combiner is null");
		
		m_rset = rset;
		m_dsIdx = dsIdx;
		m_combiner = combiner;
		
		m_inputRecord = DefaultRecord.of(rset.getRecordSchema());
		if ( m_dsIdx == 0 ) {
			m_binding.put("left", DefaultRecord.of(leftSchema));
			m_binding.put("right", DefaultRecord.of(rightSchema));
		}
		else {
			m_binding.put("left", DefaultRecord.of(leftSchema));
			m_binding.put("right", DefaultRecord.of(rightSchema));
		}
	}
	
	@Override protected void closeInGuard() { }

	@Override
	public RecordSchema getRecordSchema() {
		return m_combiner.getRecordSchema();
	}

	@Override
	public boolean next(Record output) {
		if ( !m_rset.next(m_inputRecord) ) {
			return false;
		}
		
		m_binding.put((m_dsIdx == 0) ? "left" : "right", m_inputRecord);
		m_combiner.select(m_binding, output);
		
		return true;
	}
}
