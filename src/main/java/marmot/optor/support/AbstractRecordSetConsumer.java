package marmot.optor.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.optor.RecordSetConsumer;
import marmot.support.DefaultRecord;
import utils.LoggerSettable;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AbstractRecordSetConsumer implements RecordSetConsumer, LoggerSettable {
	protected MarmotCore m_marmot;
	protected RecordSchema m_inputSchema;
	private Logger m_logger = LoggerFactory.getLogger(getClass());
	
	public final void checkInitialized() {
		if ( m_marmot == null ) {
			throw new IllegalArgumentException("not initialized: optor=" + this);
		}
	}
	
	public final boolean isInitialized() {
		return m_marmot != null;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		setInitialized(marmot, inputSchema);
	}

	@Override
	public final MarmotCore getMarmotCore() {
		if ( m_marmot == null ) {
			throw new IllegalArgumentException("not initialized: optor=" + this);
		}
		
		return m_marmot;
	}
	
	@Override
	public final RecordSchema getInputRecordSchema() {
		if ( m_inputSchema == null ) {
			throw new IllegalArgumentException("not initialized: optor=" + this);
		}
		
		return m_inputSchema;
	}

	@Override
	public final RecordSchema getRecordSchema() {
		if ( m_inputSchema == null ) {
			throw new IllegalArgumentException("not initialized: optor=" + this);
		}
		
		return m_inputSchema;
	}
	
	protected final void setInitialized(MarmotCore marmot, RecordSchema inputSchema)	{
		Utilities.checkNotNullArgument(marmot, "marmot is null");
		Utilities.checkNotNullArgument(inputSchema, "inputSchema is null");
		
		m_marmot = marmot;
		m_inputSchema = inputSchema;
	}

	@Override
	public Logger getLogger() {
		return m_logger;
	}

	@Override
	public void setLogger(Logger logger) {
		m_logger = logger;
	}
	
	@Override
	public String toString() {
		return getClass().getSimpleName();
	}
	
	protected final Record newInputRecord() {
		return DefaultRecord.of(m_inputSchema);
	}
}
