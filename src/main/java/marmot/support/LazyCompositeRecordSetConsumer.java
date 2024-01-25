package marmot.support;

import java.util.List;

import utils.stream.FStream;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.optor.CompositeRecordSetConsumer;
import marmot.optor.RecordSetOperator;
import marmot.optor.support.RecordSetOperators;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LazyCompositeRecordSetConsumer extends CompositeRecordSetConsumer {
	private enum State { NOT_INITIALIZED, PENDED, INITIALIZED };
	
	private final CompositeRecordSetConsumer m_optor;
	private State m_state;
	
	public LazyCompositeRecordSetConsumer(CompositeRecordSetConsumer consumer) {
		m_optor = consumer;
		m_state = State.NOT_INITIALIZED;
	}

	@Override
	protected void _initialize(MarmotCore marmot, RecordSchema inputSchema) {
		m_state = State.PENDED;
	}

	@Override
	protected RecordSetOperatorChain createComponents() {
		switch ( m_state ) {
			case NOT_INITIALIZED:
				throw new IllegalStateException("not initialized: optor=" + m_optor);
			case PENDED:
				m_optor.initialize(m_marmot, getInputRecordSchema());
			case INITIALIZED:
				List<RecordSetOperator> lazyOptors
							= FStream.from(m_optor.getComponents().getAll())
									.map(RecordSetOperators::toLazy)
									.toList();
				return FStream.from(lazyOptors)
								.fold(RecordSetOperatorChain.from(m_marmot, m_inputSchema),
										 (c,o) -> c.add(o));
			default:
				throw new AssertionError();
		}
	}
}
