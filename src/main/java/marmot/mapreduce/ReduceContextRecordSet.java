package marmot.mapreduce;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.ReduceContext;
import org.slf4j.Logger;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSetException;
import marmot.io.RecordWritable;
import marmot.rset.AbstractRecordSet;
import marmot.support.ProgressReportable;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ReduceContextRecordSet extends AbstractRecordSet implements ProgressReportable {
	@SuppressWarnings("rawtypes")
	private final ReduceContext m_context;
	private final RecordSchema m_schema;
	private Iterator<RecordWritable> m_iter = Collections.emptyIterator();
	
	private long m_count = 0;
	protected long m_elapsed;
	private boolean m_finalProgressReported = false;
	
	@SuppressWarnings("rawtypes")
	public ReduceContextRecordSet(ReduceContext context, RecordSchema schema) {
		m_context = context;
		m_schema = schema;
//		m_keyPresent = !NullWritable.class.isAssignableFrom(context.getMapOutputKeyClass());
	}
	
	@Override protected void closeInGuard() { }

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	public ReduceContext getReduceContext() {
		return m_context;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public boolean next(Record record) throws RecordSetException {
		checkNotClosed();
		
		if ( !m_iter.hasNext() ) {
			try {
				if ( m_iter instanceof ReduceContext.ValueIterator ) {
					((ReduceContext.ValueIterator)m_iter).resetBackupStore();
				}
				
				if ( !m_context.nextKey() ) {
					return false;
				}
				
				m_iter = m_context.getValues().iterator();
			}
			catch ( IOException | InterruptedException e ) {
				throw new RecordSetException(e);
			}
		}
		
		m_iter.next().storeTo(record);
		++m_count;
		
		return true;
	}
	
	@Override
	public String toString() {
		float prog = m_context.getProgress() * 100;
		long velo = Math.round(((double)m_count / m_elapsed) * 1000);
		return String.format("MRReducerMain: count=%d, velo=%d, progress=%.1f%%", m_count, velo, prog);
	}
	
	@Override
	public void reportProgress(Logger logger, StopWatch elapsed) {
		logger.info("report: {}: count={}", m_count);
		
		if ( !isClosed() || !m_finalProgressReported ) {
			m_elapsed = elapsed.getElapsedInMillis();
			logger.info("report: [{}]{}", isClosed() ? "C": "O", toString());
			
			if ( isClosed() ) {
				m_finalProgressReported = true;
			}
		}
	}
}
