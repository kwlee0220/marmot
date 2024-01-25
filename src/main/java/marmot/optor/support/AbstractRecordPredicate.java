package marmot.optor.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.MarmotRuntime;
import marmot.RecordPredicate;
import marmot.RecordSchema;
import utils.LoggerSettable;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AbstractRecordPredicate implements RecordPredicate, LoggerSettable {
	private RecordSchema m_inputSchema;
	private Logger m_logger = LoggerFactory.getLogger(getClass());

	protected void setInitialized(MarmotRuntime marmot, RecordSchema inputSchema) {
		m_inputSchema = inputSchema;
	}
	
	protected final void assertInitialized() {
		if ( m_inputSchema == null ) {
			throw new IllegalStateException("not initialized: predicate=" + this);
		}
	}

	@Override
	public Logger getLogger() {
		return m_logger;
	}

	@Override
	public void setLogger(Logger logger) {
		m_logger = logger;
	}
}