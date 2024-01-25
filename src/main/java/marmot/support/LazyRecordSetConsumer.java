package marmot.support;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.RecordSetConsumer;
import marmot.optor.support.AbstractRecordSetConsumer;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LazyRecordSetConsumer extends AbstractRecordSetConsumer {
	private enum State { NOT_INITIALIZED, PENDED, INITIALIZED };

	private final RecordSetConsumer m_optor;
	private State m_state;
	
	public LazyRecordSetConsumer(RecordSetConsumer consumer) {
		m_optor = consumer;
		m_state = State.NOT_INITIALIZED;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		m_state = State.PENDED;
		
		setInitialized(marmot, inputSchema);
	}

	@Override
	public void consume(RecordSet rset) {
		switch ( m_state ) {
			case NOT_INITIALIZED:
				throw new IllegalStateException("not initialized: optor=" + m_optor);
			case PENDED:
				m_optor.initialize(m_marmot, getOutputRecordSchema());
			case INITIALIZED:
				m_optor.consume(rset);
				break;
			default:
				throw new AssertionError();
		}
	}
}
