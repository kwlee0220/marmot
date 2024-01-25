package marmot.optor;

import org.slf4j.LoggerFactory;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.support.AbstractRecordSetLoader;
import marmot.support.RecordSetOperatorChain;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class CompositeRecordSetLoader extends AbstractRecordSetLoader
												implements CompositeRecordSetOperator {
	private RecordSetOperatorChain m_components = null;

	protected abstract RecordSchema _initialize(MarmotCore marmot);
	protected abstract RecordSetOperatorChain createComponents();
	
	protected CompositeRecordSetLoader() {
		setLogger(LoggerFactory.getLogger(getClass()));
	}

	@Override
	public void initialize(MarmotCore marmot) {
		RecordSchema outSchema = _initialize(marmot);
		
		setInitialized(marmot, outSchema);
	}

	@Override
	public final RecordSetOperatorChain getComponents() {
		checkInitialized();
		
		if ( m_components == null ) {
			m_components = createComponents();
			if ( m_components.length() == 0 ) {
				throw new IllegalStateException("CompositeRecordSetLoader returns "
												+ "empty components: optor=" + this);
			}

			RecordSetOperator last = m_components.getLast();
			if ( !(last instanceof RecordSetFunction) && !(last instanceof RecordSetLoader) ) {
				throw new IllegalStateException(
								"the last operator of the CompositeRecordSetLoader "
								+ "should a RecordSetFunction or RecordSetLoader: last="
								+ last + ", optor=" + this);
			}
		}
		
		return m_components;
	}

	@Override
	public final RecordSet load() {
		checkInitialized();
		
		return getComponents().run();
	}
}
