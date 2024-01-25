package marmot.support;

import java.util.List;

import com.google.common.collect.Lists;

import marmot.Plan;
import marmot.PlanBuilder;
import marmot.exec.MarmotExecutionException;
import marmot.optor.RecordSetOperator;
import marmot.optor.geo.cluster.AttachQuadKeyRSet;
import marmot.optor.reducer.RecordSetReducer;
import marmot.plan.Group;
import marmot.proto.SerializedProto;
import marmot.proto.optor.OperatorProto;
import marmot.proto.optor.PlanProto;
import marmot.proto.optor.ReducerProto;
import marmot.protobuf.PBUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PlanUtils {
	private PlanUtils() {
		throw new AssertionError("Should not be called: " + getClass().getName());
	}
	
	public static Plan toPlan(String name, List<RecordSetOperator> optors) {
		List<OperatorProto> protos = Lists.newArrayList();
		for ( RecordSetOperator optor: optors ) {
			if ( optor instanceof PBSerializable ) {
				SerializedProto serialized = ((PBSerializable<?>)optor).serialize();
				protos.add(OperatorProto.newBuilder().setSerialized(serialized).build());
			}
			else {
				throw new MarmotExecutionException(
						"RecordSetOperator is not ProtoBuf-Serializable: optor=" + optor);
			}
		}
		
		return Plan.fromProto(PlanProto.newBuilder()
										.setName(name)
										.addAllOperators(protos)
										.build());
	}
	
	public static Plan prependOperator(Plan plan, OperatorProto optor) {
		List<OperatorProto> optors = Lists.newArrayList(plan.toProto().getOperatorsList());
		optors.add(0, optor);
		
		return Plan.fromProto(PlanProto.newBuilder()
									.setName(plan.getName())
									.addAllOperators(optors)
									.build());
	}
	
	public static <T extends RecordSetReducer & PBSerializable<?>>
	PlanBuilder addReduceByGroup(PlanBuilder builder, Group group, T reducer) {
		ReducerProto proto = ReducerProto.newBuilder()
											.setReducer(PBUtils.serialize(reducer.toProto()))
											.build();
		return builder.reduceByGroup(Group.ofKeys(AttachQuadKeyRSet.COL_QUAD_KEY), proto);
	}
	
	public static <T extends RecordSetReducer & PBSerializable<?>>
	PlanBuilder addReduce(PlanBuilder builder, T reducer) {
		ReducerProto proto = ReducerProto.newBuilder()
											.setReducer(PBUtils.serialize(reducer.toProto()))
											.build();
		return builder.add(OperatorProto.newBuilder()
										.setReduce(proto)
										.build());
	}
}
