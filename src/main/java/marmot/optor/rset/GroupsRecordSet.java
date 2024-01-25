package marmot.optor.rset;

import org.slf4j.Logger;

import marmot.RecordSchema;
import marmot.optor.RecordSetOperator;
import marmot.optor.support.KeyedRecordSet;
import marmot.optor.support.KeyedRecordSetFactory;
import marmot.rset.AbstractRecordSet;
import marmot.support.ProgressReportable;
import utils.StopWatch;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class GroupsRecordSet<T extends RecordSetOperator> extends AbstractRecordSet
																implements ProgressReportable {
	protected final T m_optor;
	private final KeyedRecordSetFactory m_groups;
	
	private int m_groupCount = 0;
	private boolean m_finalProgressReported = false;
	
	protected abstract long getOutputCount();
	
	protected GroupsRecordSet(T optor, KeyedRecordSetFactory groups) {
		m_optor = optor;
		m_groups = groups;
	}

	@Override
	protected void closeInGuard() {
		m_groups.closeQuietly();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_optor.getRecordSchema();
	}
	
	protected FOption<KeyedRecordSet> nextGroup() {
		return m_groups.nextKeyedRecordSet()
						.ifPresent(r -> ++m_groupCount);
	}

	@Override
	public void reportProgress(Logger logger, StopWatch elapsed) {
		if ( !isClosed() || !m_finalProgressReported ) {
			m_groups.reportProgress(logger, elapsed);
			
			logger.info("report: [{}]{}", isClosed() ? "C": "O", toString());
			
			if ( isClosed() ) {
				m_finalProgressReported = true;
			}
		}
	}
	
	@Override
	public String toString() {
		return String.format("%s: %d->%d", m_optor, m_groupCount, getOutputCount());
	}
}
