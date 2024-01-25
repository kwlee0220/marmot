package marmot.optor;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.support.RecordSetOperatorChain;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class CompositeRecordSetFunction extends AbstractRecordSetFunction
												implements CompositeRecordSetOperator {
	protected RecordSetOperatorChain m_components = null;
	
	protected abstract RecordSchema _initialize(MarmotCore marmot, RecordSchema inputSchema);
	protected abstract RecordSetOperatorChain createComponents();

	@Override
	public final void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		RecordSchema outSchema = _initialize(marmot, inputSchema);
		
		setInitialized(marmot, inputSchema, outSchema);
	}

	@Override
	public final RecordSetOperatorChain getComponents() {
		checkInitialized();
		
		if ( m_components == null ) {
			m_components = createComponents();
			if ( m_components.length() == 0 ) {
				throw new IllegalStateException("CompositeRecordSetFunction returns "
												+ "empty components: optor=" + this);
			}

			RecordSetOperator last = m_components.getLast();
			if ( !(last instanceof RecordSetFunction) && !(last instanceof RecordSetLoader) ) {
				throw new IllegalStateException(
								"the last operator of the CompositeRecordSetFunction "
								+ "should a RecordSetFunction or RecordSetLoader: last="
								+ last + ", optor=" + this);
			}
		}
		
		return m_components;
	}

	@Override
	public final RecordSet apply(RecordSet rset) {
		checkInitialized();
		
		return getComponents().run(rset);
	}
}
