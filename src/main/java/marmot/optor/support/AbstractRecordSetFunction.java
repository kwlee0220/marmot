package marmot.optor.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.optor.RecordSetFunction;
import marmot.support.DefaultRecord;
import utils.LoggerSettable;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AbstractRecordSetFunction implements RecordSetFunction, LoggerSettable {
	protected MarmotCore m_marmot;
	protected RecordSchema m_inputSchema;
	protected RecordSchema m_outputSchema;
	private Logger m_logger = LoggerFactory.getLogger(getClass());
	
	public final void checkInitialized() {
		if ( m_outputSchema == null ) {
			throw new IllegalArgumentException("not initialized: optor=" + this);
		}
	}
	
	public final boolean isInitialized() {
		return m_marmot != null;
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
		if ( m_outputSchema == null ) {
			throw new IllegalArgumentException("not initialized: optor=" + this);
		}
		
		return m_outputSchema;
	}
	
	protected final void setInitialized(MarmotCore marmot, RecordSchema inputSchema,
										RecordSchema outputSchema)	{
//		Utilities.checkNotNullArgument(marmot, "marmot is null");
		Utilities.checkNotNullArgument(inputSchema, "inputSchema is null");
		Utilities.checkNotNullArgument(outputSchema, "outputSchema is null");
		
		m_marmot = marmot;
		m_inputSchema = inputSchema;
		m_outputSchema = outputSchema;
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
