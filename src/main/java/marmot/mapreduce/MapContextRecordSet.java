package marmot.mapreduce;

import org.apache.hadoop.mapreduce.MapContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class MapContextRecordSet extends AbstractRecordSet implements ProgressReportable {
	private final RecordSchema m_schema;
	private final MapContext<?, ?, ?, ?> m_context;
	private long m_count;
	
	public MapContextRecordSet(RecordSchema schema,
								MapContext<?, ?, ?, ?> context) {
		m_schema = schema;
		m_context = context;
		
		m_count = 0;
		
		setLogger(LoggerFactory.getLogger(MapContextRecordSet.class));
	}
	
	@Override
	protected void closeInGuard() {}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}

	@Override
	public boolean next(Record record) {
		checkNotClosed();
		
		try {
			if ( !m_context.nextKeyValue() ) {
				return false;
			}
			
			final Object value = m_context.getCurrentValue();
			if ( value instanceof Record ) {
				record.set((Record)value);
			}
			else if ( value instanceof RecordWritable ) {
				record.setAll(((RecordWritable)value).get());
			}
			else {
				throw new AssertionError("unexpected Mapper's value object: value=" + value);
			}
			++m_count;
			
			return true;
		}
		catch ( Exception e ) {
			throw new RecordSetException("fails to get a record from MapContext, cause=" + e);
		}
	}

	@Override
	public Record nextCopy() {
		checkNotClosed();
		
		try {
			if ( !m_context.nextKeyValue() ) {
				return null;
			}
			
			final Object value = m_context.getCurrentValue();
			if ( value instanceof Record ) {
				++m_count;
				return (Record)value;
			}
			else if ( value instanceof RecordWritable ) {
				++m_count;
				return ((RecordWritable)value).toRecord(m_schema);
			}
			else {
				throw new AssertionError("unexpected Mapper's value object: value=" + value);
			}
		}
		catch ( Exception e ) {
			throw new RecordSetException("fails to get a record from MapContext, cause=" + e);
		}
	}
	
	@Override
	public void reportProgress(Logger logger, StopWatch elapsed) {
		String ocStr = isClosed() ? "C" : "O";
		logger.info("report: [{}]{}", ocStr, this);
	}
	
	@Override
	public String toString() {
		return String.format("mapper: nread=%d, progress=%.2f%%", m_count, m_context.getProgress()*100);
	}
}