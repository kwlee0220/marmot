package marmot.optor.rset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.RecordSetFunction;
import marmot.rset.AbstractRecordSet;
import marmot.support.DefaultRecord;
import marmot.support.ProgressReportable;
import utils.StopWatch;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class SingleInputRecordSet<T extends RecordSetFunction>
										extends AbstractRecordSet implements ProgressReportable {
	protected final T m_optor;
	protected final RecordSet m_input;
	protected final RecordSchema m_outSchema;
	protected final RecordSchema m_inputSchema;
	
	protected long m_elapsed;
	private boolean m_finalProgressReported = false;
	
	protected SingleInputRecordSet(T func, RecordSet input) {
		Utilities.checkNotNullArgument(func, "func is null");
		Utilities.checkNotNullArgument(input, "input is null");
		
		m_optor = func;
		m_input = input;
		m_inputSchema = input.getRecordSchema();
		m_outSchema = func.getRecordSchema();
		
		setLogger(LoggerFactory.getLogger(func.getClass()));
	}

	@Override
	protected void closeInGuard() {
		m_input.closeQuietly();
		
		getLogger().debug("done: {}", this);
	}
	
	public RecordSet getInputRecordSet() {
		return m_input;
	}
	
	public RecordSchema getInputSchema() {
		return m_inputSchema;
	}
	
	@Override
	public RecordSchema getRecordSchema() {
		return m_outSchema;
	}
	
	@Override
	public String toString() {
		return m_optor.toString();
	}
	
	protected DefaultRecord newInputRecord() {
		return DefaultRecord.of(m_inputSchema);
	}
	
	@Override
	public void reportProgress(Logger logger, StopWatch elapsed) {
		if ( !isClosed() || !m_finalProgressReported ) {
			if ( m_input instanceof ProgressReportable ) {
				((ProgressReportable)m_input).reportProgress(logger, elapsed);
			}
			
			m_elapsed = elapsed.getElapsedInMillis();
			logger.info("report: [{}]{}", isClosed() ? "C": "O", toString());
			
			if ( isClosed() ) {
				m_finalProgressReported = true;
			}
		}
	}
}
