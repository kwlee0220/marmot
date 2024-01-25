package marmot.optor.join;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import marmot.Record;
import marmot.RecordSchema;
import marmot.optor.support.colexpr.ColumnSelector;
import marmot.rset.AbstractRecordSet;
import marmot.support.DefaultRecord;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class PartitionOuterJoin extends AbstractRecordSet {
	private final Iterator<Record> m_dsIter;
	private final boolean m_swapped;
	private final ColumnSelector m_combiner;
	private final Map<String,Record> m_binding = Maps.newHashMap();
	
	PartitionOuterJoin(List<Record> dataset, RecordSchema nullSchema, ColumnSelector combiner,
						boolean swapped) {
		Preconditions.checkArgument(dataset != null);
		Preconditions.checkArgument(nullSchema != null);
		
		m_dsIter = dataset.iterator();
		m_swapped = swapped;
		m_combiner = combiner;

		Record nullRecord = DefaultRecord.of(nullSchema);
		if ( m_swapped ) {
			m_binding.put("left", nullRecord);
		}
		else {
			m_binding.put("right", nullRecord);
		}
	}
	
	@Override protected void closeInGuard() { }

	@Override
	public RecordSchema getRecordSchema() {
		return m_combiner.getRecordSchema();
	}

	@Override
	public boolean next(Record output) {
		if ( !m_dsIter.hasNext() ) {
			return false;
		}

		Record record = m_dsIter.next();
		if ( m_swapped ) {
			m_binding.put("right", record);
		}
		else {
			m_binding.put("left", record);
		}
		m_combiner.select(m_binding, output);
		
		return true;
	}
}
