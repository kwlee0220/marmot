package marmot.optor;

import marmot.support.RecordSetOperatorChain;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface CompositeRecordSetOperator extends RecordSetOperator {
	public RecordSetOperatorChain getComponents();
}
