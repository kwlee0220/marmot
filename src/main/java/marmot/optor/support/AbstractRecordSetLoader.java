package marmot.optor.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.optor.RecordSetLoader;
import utils.LoggerSettable;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AbstractRecordSetLoader implements RecordSetLoader, LoggerSettable {
	protected MarmotCore m_marmot;
	private RecordSchema m_outputSchema;
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
	public final MarmotCore getMarmotCore() {
		checkInitialized();
		
		return m_marmot;
	}

	@Override
	public final RecordSchema getRecordSchema() {
		if ( m_outputSchema == null ) {
			throw new IllegalArgumentException("not initialized: optor=" + this);
		}
		
		return m_outputSchema;
	}
	
	protected final void setInitialized(MarmotCore marmot, RecordSchema outputSchema)	{
		Utilities.checkNotNullArgument(marmot, "marmot is null");
		Utilities.checkNotNullArgument(outputSchema, "outputSchema is null");
		
		m_marmot = marmot;
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
}
