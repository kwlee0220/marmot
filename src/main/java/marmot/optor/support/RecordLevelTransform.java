package marmot.optor.support;

import marmot.Record;
import marmot.RecordSet;
import marmot.RecordTransform;
import marmot.optor.rset.SingleInputRecordSet;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class RecordLevelTransform extends AbstractRecordSetFunction
												implements RecordTransform {
	@Override
	public RecordSet apply(RecordSet input) {
		checkInitialized();
		
		return new Transformed(this, input);
	}

	private static class Transformed extends SingleInputRecordSet<RecordLevelTransform> {
		private final Record m_inputRecord;
		private long m_count =0;
		private long m_dropCount =0;
		private long m_failCount =0;
		
		Transformed(RecordLevelTransform optor, RecordSet input) {
			super(optor, input);
			
			m_inputRecord = newInputRecord();
		}
		
		@Override
		public boolean next(Record record) {
			while ( m_input.next(m_inputRecord) ) {
				++m_count;
				try {
					if ( m_optor.transform(m_inputRecord, record) ) {
						return true;
					}
					++m_dropCount;
				}
				catch ( Throwable e ) {
					getLogger().warn("ignored transform failure: op=" + this + ", cause=" + e);
					++m_failCount;
				}
			}
			
			return false;
		}
		
		@Override
		public String toString() {
			return String.format("%s: count=%d dropped=%d failed=%d",
								m_optor, m_count, m_dropCount, m_failCount);
		}
	}
}
