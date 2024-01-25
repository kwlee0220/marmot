package marmot.optor.reducer;

import java.io.Serializable;

import marmot.optor.RecordSetConsumer;
import marmot.optor.RecordSetFunction;
import marmot.optor.StoreKeyedDataSet;
import marmot.proto.SerializedProto;
import marmot.proto.optor.GroupConsumerProto;
import marmot.proto.optor.ReducerProto;
import marmot.proto.optor.StoreKeyedDataSetProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class GroupByProtoUtils {
	private GroupByProtoUtils() {
		throw new AssertionError("Should not be called: class=" + GroupByProtoUtils.class);
	}
	
	public static RecordSetFunction fromProto(ReducerProto proto) {
		switch ( proto.getOneofReducerCase() ) {
			case VAL_AGGREGATES:
				return ValueAggregateReducer.fromProto(proto.getValAggregates());
			case LIST:
				return ListReducer.get();
			case TAKE:
				return TakeReducer.fromProto(proto.getTake());
			case RUN_PLAN:
				return RunPlanReducer.fromProto(proto.getRunPlan());
			case PUT_SIDE_BY_SIDE:
				return PutSideBySide.fromProto(proto.getPutSideBySide());
			case REDUCER:
				return PBUtils.deserialize(proto.getReducer());
			default:
				throw new IllegalArgumentException("unknown ReducerProto: proto=" + proto);
		}
	}
	
	public static ReducerProto toProto(RecordSetFunction func) {
		if ( func instanceof ValueAggregateReducer ) {
			return toProto((ValueAggregateReducer)func);
		}
		else if ( func instanceof ListReducer ) {
			return ReducerProto.newBuilder().setList(ListReducer.getProto()).build();
		}
		else if ( func instanceof TakeReducer ) {
			return ReducerProto.newBuilder()
								.setTake(((TakeReducer)func).toProto())
								.build();
		}
		else if ( func instanceof RunPlanReducer ) {
			return ReducerProto.newBuilder()
								.setRunPlan(((RunPlanReducer)func).toProto())
								.build();
		}
		else if ( func instanceof PutSideBySide ) {
			return ReducerProto.newBuilder()
								.setPutSideBySide(((PutSideBySide)func).toProto())
								.build();
		}
		else if ( func instanceof PBSerializable ) {
			return ReducerProto.newBuilder()
								.setReducer(((PBSerializable<?>)func).serialize())
								.build();
		}
		else if ( func instanceof Serializable ) {
			return toProto(PBUtils.serializeJava((Serializable)func));
		}
		else {
			throw new IllegalArgumentException("invalid RecordSetFunction: func=" + func);
		}
	}
	
	public static RecordSetConsumer fromProto(GroupConsumerProto proto) {
		switch ( proto.getOneofConsumerCase() )  {
			case STORE:
				return StoreKeyedDataSet.fromProto(proto.getStore());
			case SERIALIZED:
				return PBUtils.deserialize(proto.getSerialized());
			default:
				throw new IllegalArgumentException("unknown GroupConsumerProto: proto=" + proto);
		}
	}
	
	public static GroupConsumerProto toProto(RecordSetConsumer consumer) {
		if ( consumer instanceof StoreKeyedDataSet ) {
			StoreKeyedDataSetProto store = ((StoreKeyedDataSet)consumer).toProto();
			return GroupConsumerProto.newBuilder().setStore(store).build();
		}
		else if ( consumer instanceof PBSerializable ) {
			return GroupConsumerProto.newBuilder()
									.setSerialized(((PBSerializable<?>)consumer).serialize())
									.build();
			}
		else if ( consumer instanceof Serializable ) {
			SerializedProto proto = PBUtils.serializeJava((Serializable)consumer);
			return GroupConsumerProto.newBuilder()
									.setSerialized(proto)
									.build();
		}
		else {
			throw new IllegalArgumentException("invalid RecordSetFunction: func=" + consumer);
		}
	}
	
	public static ReducerProto toProto(ValueAggregateReducer reducer) {
		return ReducerProto.newBuilder()
							.setValAggregates(reducer.toProto())
							.build();
	}
	
	public static ReducerProto toProto(SerializedProto serialized) {
		return ReducerProto.newBuilder()
							.setReducer(serialized)
							.build();
	}
}
