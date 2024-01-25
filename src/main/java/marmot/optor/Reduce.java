package marmot.optor;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.reducer.AggregateIntermAtMapSide;
import marmot.optor.reducer.CombineableRecordSetReducer;
import marmot.optor.reducer.ValueAggregateReducer;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.proto.optor.ReducerProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Reduce extends AbstractRecordSetFunction implements PBSerializable<ReducerProto> {
	private final RecordSetFunction m_reducer;
	
	public Reduce(RecordSetFunction reducer) {
		m_reducer = reducer;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		m_reducer.initialize(marmot, inputSchema);
		
		setInitialized(marmot, inputSchema, m_reducer.getRecordSchema());
	}

	@Override
	public MapReduceJoint getMapReduceJoint(RecordSchema inputSchema) {
		checkInitialized();
		
		MapReduceJoint joint = MapReduceJoint.create().setReducerCount(1);
		
		if ( m_reducer instanceof ValueAggregateReducer ) {
			ValueAggregateReducer combineable = (ValueAggregateReducer)m_reducer;
			
			joint.addMapper(combineable.newIntermediateProducer());
			
			// mapper 단계에서 group 별로 1차 aggregation을 수행함
			AggregateIntermAtMapSide intraPartReduce
			                = new AggregateIntermAtMapSide(combineable.newIntermediateReducer());
			joint.addMapper(intraPartReduce);
			
			// reducer 단계에서 group별로 combine을 수행하게 함
			joint.addReducer(combineable.newIntermediateReducer());
			
			// 누적된 임시 데이터를 최종 결과를 변경시킴
			joint.addReducer(combineable.newIntermediateFinalizer());
		}
		else if ( m_reducer instanceof CombineableRecordSetReducer ) {
			CombineableRecordSetReducer combineable = (CombineableRecordSetReducer)m_reducer;
			
			joint.addMapper(combineable.newIntermediateProducer());
			joint.addCombiner(combineable.newIntermediateReducer());
			joint.addReducer(combineable.newIntermediateReducer());
			joint.addReducer(combineable.newIntermediateFinalizer());
		}
		else {
			joint.addReducer(this);
		}
		
		return joint;
	}

	@Override
	public RecordSet apply(RecordSet input) {
		checkInitialized();
		
		return m_reducer.apply(input);
	}

	@Override
	public String toString() {
		return String.format("%s[reducer=%s]", getClass().getSimpleName(), m_reducer);
	}

	public static Reduce fromProto(ReducerProto proto) {
		RecordSetFunction reducer = null;
		switch ( proto.getOneofReducerCase() ) {
			case VAL_AGGREGATES:
				reducer = ValueAggregateReducer.fromProto(proto.getValAggregates());
				break;
			case REDUCER:
				reducer = PBUtils.deserialize(proto.getReducer());
				break;
			default:
				throw new AssertionError();
		}
		
		return new Reduce(reducer);
	}
	
	@Override
	public ReducerProto toProto() {
		ReducerProto.Builder builder = ReducerProto.newBuilder();
		if ( m_reducer instanceof ValueAggregateReducer ) {
			builder.setValAggregates(((ValueAggregateReducer)m_reducer).toProto());
		}
		else {
			builder.setReducer(PBUtils.serialize(m_reducer));
		}
		return builder.build();
	}
}
