package marmot.optor.rset;

import marmot.Record;
import marmot.RecordSet;
import marmot.optor.RecordSetFunction;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class MultiValuedRecordSet<T extends RecordSetFunction>
													extends SingleInputRecordSet<T> {
	private RecordSet m_pended;
	
	protected abstract RecordSet nextValues();

	protected MultiValuedRecordSet(T func, RecordSet input) {
		super(func, input);
		
		m_pended = RecordSet.empty(getRecordSchema());
	}

	@Override
	public boolean next(Record record) {
		if ( m_pended == null ) {
			return false;
		}
		
		while ( !m_pended.next(record) ) {
			if ( (m_pended = nextValues()) == null ) {
				return false;
			}
		}
		
		return true;
	}
}
