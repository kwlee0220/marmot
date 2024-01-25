package marmot.optor;

import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.support.AbstractRecordSetConsumer;
import marmot.support.RecordSetOperatorChain;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class CompositeRecordSetConsumer extends AbstractRecordSetConsumer
												implements CompositeRecordSetOperator {
	private RecordSetOperatorChain m_components = null;
	
	protected abstract void _initialize(MarmotCore marmot, RecordSchema inputSchema);
	protected abstract RecordSetOperatorChain createComponents();
	
	protected CompositeRecordSetConsumer() {
		setLogger(LoggerFactory.getLogger(getClass()));
	}

	@Override
	public final void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		_initialize(marmot, inputSchema);
		
		setInitialized(marmot, inputSchema);
	}

	@Override
	public final RecordSetOperatorChain getComponents() {
		checkInitialized();
		
		if ( m_components == null ) {
			m_components = createComponents();
			Preconditions.checkState(m_components.length() > 0,
								"CompositeRecordSetConsumer returns empty components: optor=" + this);

			RecordSetOperator last = m_components.getLast();
			if ( !(last instanceof RecordSetConsumer) ) {
				throw new IllegalStateException("the last operator of the CompositeRecordSetConsumer "
												+ "should a RecordSetConsumer: last=" + last
												+ ", optor=" + this);
			}
		}
		
		return m_components;
	}

	@Override
	public void consume(RecordSet rset) {
		checkInitialized();

		getComponents().run(rset);
	}
}
