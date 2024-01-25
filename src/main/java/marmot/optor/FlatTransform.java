package marmot.optor;

import org.slf4j.LoggerFactory;

import marmot.Record;
import marmot.RecordFlatTransform;
import marmot.RecordSet;
import marmot.optor.rset.SingleInputRecordSet;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.rset.FlatTransformedRecordSet;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class FlatTransform extends AbstractRecordSetFunction
									implements RecordFlatTransform {
	protected FlatTransform() {
		setLogger(LoggerFactory.getLogger(FlatTransform.class));
	}

	@Override
	public RecordSet apply(RecordSet input) {
		checkInitialized();
		Utilities.checkNotNullArgument(input, "input is null");
		
		return new FlatTransformed(this, input);
	}
	
	private static class FlatTransformed extends SingleInputRecordSet<FlatTransform> {
		private final FlatTransformedRecordSet m_output;
		
		protected FlatTransformed(FlatTransform func, RecordSet input) {
			super(func, input);
			
			m_output = input.flatMap(getRecordSchema(), func::transform);
		}

		@Override
		protected void closeInGuard() {
			m_output.closeQuietly();
		}
		
		@Override
		public boolean next(Record record) {
			return m_output.next(record);
		}
		
		@Override
		public String toString() {
			return String.format("%s: in_count=%d out_count=%d",
									m_optor, m_output.getRecordInputCount(),
									m_output.getRecordOutputCount());
		}
	}
}
