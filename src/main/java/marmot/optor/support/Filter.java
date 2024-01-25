package marmot.optor.support;

import org.slf4j.LoggerFactory;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordPredicate;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.rset.SingleInputRecordSet;
import marmot.plan.PredicateOptions;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class Filter<T extends Filter<T>> extends AbstractRecordSetFunction
													implements RecordPredicate {
	protected final PredicateOptions m_options;
	private final boolean m_negated;
	
	protected Filter(PredicateOptions opts) {
		m_options = opts;
		
		m_negated = opts.negated().getOrElse(false);
		setLogger(LoggerFactory.getLogger(Filter.class));
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		setInitialized(marmot, inputSchema, inputSchema);
	}

	@Override
	public RecordSet apply(RecordSet input) {
		checkInitialized();
		
		return new Filtered<>(this, input);
	}
	
	@Override
	public String toString() {
		return getClass().getSimpleName();
	}
	
	protected boolean getNegated() {
		return m_negated;
	}
	
	static class Filtered<T extends Filter<T>> extends SingleInputRecordSet<Filter<T>> {
		private long m_inCount =0;
		private long m_outCount =0;
		private long m_failCount =0;
		
		Filtered(Filter<T> filter, RecordSet input) {
			super(filter, input);
		}
		
		@Override
		public boolean next(Record record) {
			checkNotClosed();
			
			while ( m_input.next(record) ) {
				++m_inCount;
				try {
					boolean ret = m_optor.test(record);
					ret = m_optor.m_negated ? !ret : ret;
					
					if ( ret ) {
						++m_outCount;
						return true;
					}
				}
				catch ( Throwable e ) {
					getLogger().warn("ignored failure: op=" + this, e);
					++m_failCount;
				}
			}
			
			return false;
		}
		
		@Override
		public String toString() {
			String failStr = m_failCount > 0 ? String.format(" (fails=%d)", m_failCount) : "";
			double percent = ((double)m_outCount / m_inCount) * 100;
			return String.format("%s: %d -> %d(%.2f%%)%s",
								m_optor, m_inCount, m_outCount, percent, failStr);
		}
	}
}