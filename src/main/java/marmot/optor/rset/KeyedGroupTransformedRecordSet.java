package marmot.optor.rset;

import org.slf4j.Logger;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.RecordSetFunction;
import marmot.optor.support.KeyedRecordSet;
import marmot.optor.support.KeyedRecordSetFactory;
import marmot.rset.ConcatedRecordSet;
import marmot.support.ProgressReportable;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class KeyedGroupTransformedRecordSet extends ConcatedRecordSet
											implements ProgressReportable {
	private final String m_name;
	private final KeyedRecordSetFactory m_groups;
	private final RecordSetFunction m_func;
	private final RecordSchema m_outSchema;

	private volatile RecordSet m_transformed;
	private int m_ngroups = 0;
	private long m_noutputs = 0;
	private long m_elapsed;
	private boolean m_finalProgressReported = false;
	private StopWatch m_watch = StopWatch.start();
	
	public KeyedGroupTransformedRecordSet(String name, KeyedRecordSetFactory groups,
											RecordSetFunction func) {
		m_name = name;
		m_groups = groups;
		m_func = func;
		m_outSchema = RecordSchema.concat(groups.getKeySchema(), func.getRecordSchema());
	}

	@Override
	protected void closeInGuard() {
		m_groups.closeQuietly();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_outSchema;
	}

	@Override
	protected RecordSet loadNext() {
		RecordSet rset = m_groups.nextKeyedRecordSet()
									.map(this::apply)
									.map(KeyedRecordSet::flatten)
									.getOrNull();
		if ( rset != null ) {
			++m_ngroups;
			m_transformed = rset;
		}
		
		return rset;
	}

	@Override
	public boolean next(Record output) {
		if ( super.next(output) ) {
			++m_noutputs;
			
			return true;
		}
		else {
			return false;
		}
	}

	@Override
	public void reportProgress(Logger logger, StopWatch elapsed) {
		RecordSet current = m_transformed;
		if ( current != null || !m_finalProgressReported ) {
			if ( current != null && current instanceof ProgressReportable ) {
				((ProgressReportable)current).reportProgress(logger, elapsed);
			}
			
			m_elapsed = elapsed.getElapsedInMillis();
			logger.info("report: [{}]{}", isClosed() ? "C": "O", toString());
			
			if ( current == null ) {
				m_finalProgressReported = true;
			}
		}
	}
	
	@Override
	public String toString() {
		long velo = Math.round(m_noutputs / m_watch.getElapsedInFloatingSeconds());
		return String.format("%s: ngroups=%d, output=%d, velo=%d/s",
							m_name, m_ngroups, m_noutputs, velo);
	}
	
	private KeyedRecordSet apply(KeyedRecordSet krset) {
		m_transformed = m_func.apply(krset);
		return new SimpleKeyedRecordSet(krset.getKey(), krset.getKeySchema(), m_transformed);
	}
}
