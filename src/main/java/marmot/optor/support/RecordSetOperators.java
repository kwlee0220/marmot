package marmot.optor.support;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Iterables;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.RecordSets;
import marmot.optor.CompositeRecordSetConsumer;
import marmot.optor.RecordSetConsumer;
import marmot.optor.RecordSetFunction;
import marmot.optor.RecordSetLoader;
import marmot.optor.RecordSetOperator;
import marmot.support.LazyCompositeRecordSetConsumer;
import marmot.support.LazyRecordSetConsumer;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordSetOperators {
	private RecordSetOperators() {
		throw new AssertionError("Should not be called: " + getClass().getName());
	}
	
	public static RecordSetOperator toLazy(RecordSetOperator optor) {
		if ( optor instanceof CompositeRecordSetConsumer ) {
			return new LazyCompositeRecordSetConsumer((CompositeRecordSetConsumer)optor);
		}
		if ( optor instanceof RecordSetConsumer ) {
			return new LazyRecordSetConsumer((RecordSetConsumer)optor);
		}
		
		throw new AssertionError();
	}
	
	public static RecordSchema getOutputRecordSchema(RecordSetOperator optor) {
		if ( optor instanceof RecordSetLoader ) {
			return ((RecordSetLoader)optor).getRecordSchema();
		}
		else if ( optor instanceof RecordSetConsumer ) {
			return RecordSchema.NULL;
		}
		else if ( optor instanceof RecordSetFunction ) {
			return ((RecordSetFunction)optor).getRecordSchema();
		}
		else {
			throw new RecordSetException("unexpected RecordSetOperator: optor=" + optor);
		}
	}
	
	public static RecordSchema getOutputRecordSchema(MarmotCore marmot,
													List<RecordSetOperator> optorList,
													RecordSchema inputSchema) {
		initialize(marmot, optorList, inputSchema);
		return (optorList.isEmpty()) ? inputSchema : Iterables.getLast(optorList).getRecordSchema();
	}
	
	public static void initialize(MarmotCore marmot, List<RecordSetOperator> optorList,
									RecordSchema inputSchema) {
		for ( RecordSetOperator optor: optorList ) {
			initialize(marmot, optor, inputSchema);
			inputSchema = optor.getRecordSchema();
		}
	}
	
	public static void initialize(MarmotCore marmot, List<RecordSetOperator> optorList) {
		initialize(marmot, optorList, null);
	}

	public static RecordSet run(MarmotCore marmot, RecordSetOperator optor, RecordSet input) {
		if ( optor instanceof RecordSetLoader ) {
			if ( input instanceof RecordSet ) {
				throw new RecordSetException("Previous RecordSet has not been collected yet");
			}

			return ((RecordSetLoader)optor).load();
		}
		else if ( optor instanceof RecordSetConsumer ) {
			if ( input == null ) {
				// 이전 연산자가 RecordSetConsumer인 경우
				// empty RecordSet을 사용함.
				return RecordSets.NULL;
			}
			else if ( !(input instanceof RecordSet) ) {
				throw new RecordSetException("Previous RecordSet does not produce RecordSet");
			}
			((RecordSetConsumer)optor).consume((RecordSet)input);
			
			return null;
		}
		else if ( optor instanceof RecordSetFunction ) {
			if ( !(input instanceof RecordSet) ) {
				throw new RecordSetException("RecordSetFunction does not have input RecordSet: "
											+ "optor=" + optor);
			}
			return ((RecordSetFunction)optor).apply(input);
		}
		else {
			throw new RecordSetException("unexpected operator type: optor=" + optor);
		}
	}

	public static RecordSet run(MarmotCore marmot, List<RecordSetOperator> optors,
								RecordSet input) {
		RecordSet rset = input;
		for ( RecordSetOperator optor: optors ) {
			rset = run(marmot, optor, rset);
		}
	
		return rset;
	}

	public static RecordSet run(MarmotCore marmot, RecordSetOperator... optors) {
		return run(marmot, Arrays.asList(optors));
	}

	public static RecordSet run(MarmotCore marmot, List<RecordSetOperator> optorList) {
		return run(marmot, optorList, null);
	}
	
	public static void initialize(MarmotCore marmot, RecordSetOperator optor,
									RecordSchema inputSchema) {
		if ( optor instanceof RecordSetLoader ) {
			((RecordSetLoader)optor).initialize((MarmotCore)marmot);
		}
		else if ( optor instanceof RecordSetConsumer ) {
			((RecordSetConsumer)optor).initialize((MarmotCore)marmot, inputSchema);
		}
		else if ( optor instanceof RecordSetFunction ) {
			if ( inputSchema == null ) {
				throw new RecordSetException("RecordSetOperator does not have input RecordSchema: "
											+ "optor=" + optor);
			}
			
			((RecordSetFunction)optor).initialize(marmot, inputSchema);
		}
		else {
			throw new RecordSetException("unexpected operator: optor=" + optor);
		}
	}
}
