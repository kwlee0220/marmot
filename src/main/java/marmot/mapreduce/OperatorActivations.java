package marmot.mapreduce;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSetException;
import marmot.mapreduce.MultiJobPlanExecution.StageContext;
import marmot.optor.CompositeRecordSetOperator;
import marmot.optor.RecordSetConsumer;
import marmot.optor.RecordSetFunction;
import marmot.optor.RecordSetLoader;
import marmot.optor.RecordSetOperator;
import marmot.support.RecordSetOperatorChain;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class OperatorActivations {
	private final MarmotCore m_marmot;
	private final List<RecordSetOperator> m_optors;
	
	static OperatorActivations from(RecordSetOperatorChain chain) {
		return new OperatorActivations(chain.getMarmotServer(), chain.getAll());
	}
	
	private OperatorActivations(MarmotCore marmot, List<RecordSetOperator> activations) {
		m_marmot = marmot;
		m_optors = Lists.newArrayList(activations);
	}
	
	void add(int idx, RecordSetOperator optor) {
		m_optors.add(idx, optor);
	}
	
	RecordSetOperator getLeafOperatorAt(int index, RecordSchema inputSchema, StageContext cxt) {
		while ( true ) {
			if ( m_optors.size() <= index ) {
				return null;
				
			}
			RecordSetOperator optor = m_optors.get(index);
			
			if ( !optor.isInitialized() ) {
				StageWorkspaceAware.set(optor, cxt.m_stageWorkspace);
				initialize(optor, inputSchema);
			}
			
			if ( !(optor instanceof CompositeRecordSetOperator) ) {
				return optor;
			}
			
			CompositeRecordSetOperator composite = (CompositeRecordSetOperator)optor;
			
			m_optors.remove(index);
			m_optors.addAll(index, composite.getComponents().getAll());
			
			return getLeafOperatorAt(index, inputSchema, cxt);
		}
	}
	
	List<RecordSetOperator> subList(int begin, int end) {
		return Collections.unmodifiableList(m_optors.subList(begin, end));
	}
	
	int length() {
		return m_optors.size();
	}
	
	@Override
	public String toString() {
		return FStream.from(m_optors)
						.map(optor -> (optor.isInitialized() ? "(I)" : "(N)") + optor)
						.join(System.lineSeparator());
	}
	
	private void initialize(RecordSetOperator optor, RecordSchema inputSchema) {
		if ( optor instanceof RecordSetLoader ) {
			((RecordSetLoader)optor).initialize(m_marmot);
		}
		else if ( optor instanceof RecordSetConsumer ) {
			((RecordSetConsumer)optor).initialize(m_marmot, inputSchema);
		}
		else if ( optor instanceof RecordSetFunction ) {
			if ( inputSchema == null ) {
				throw new RecordSetException("RecordSetOperator does not have input RecordSchema: "
											+ "optor=" + optor);
			}
			
			((RecordSetFunction)optor).initialize(m_marmot, inputSchema);
		}
		else {
			throw new RecordSetException("unexpected operator: optor=" + optor);
		}
	}
}
