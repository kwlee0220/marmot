package marmot.optor.reducer;

import marmot.MarmotCore;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.mapreduce.MarmotMRContext;
import marmot.mapreduce.MarmotMRContexts;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.proto.optor.RunPlanProto;
import marmot.support.PBSerializable;
import marmot.support.RecordSetOperatorChain;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RunPlanReducer extends AbstractRecordSetFunction
							implements PBSerializable<RunPlanProto> {
	private final Plan m_plan;
	
	public RunPlanReducer(Plan plan) {
		m_plan = plan;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		RecordSetOperatorChain chain = RecordSetOperatorChain.from(marmot, m_plan, inputSchema);
		
		setInitialized(marmot, inputSchema, chain.getOutputRecordSchema());
	}

	@Override
	public RecordSet apply(RecordSet input) {
		checkInitialized();

		RecordSetOperatorChain chain = RecordSetOperatorChain.from(getMarmotCore(), m_plan,
																	getInputRecordSchema());
		FOption<MarmotMRContext> context = MarmotMRContexts.get();
		try {
			MarmotMRContexts.unset();
			return chain.run(input);
		}
		finally {
			context.ifPresent(cnxt -> MarmotMRContexts.set(cnxt));
		}
	}
	
	@Override
	public String toString() {
		return m_plan.toString();
	}

	public static RunPlanReducer fromProto(RunPlanProto proto) {
		return new RunPlanReducer(Plan.fromProto(proto.getPlan()));
	}

	@Override
	public RunPlanProto toProto() {
		return RunPlanProto.newBuilder()
							.setPlan(m_plan.toProto())
							.build();
	}
}
