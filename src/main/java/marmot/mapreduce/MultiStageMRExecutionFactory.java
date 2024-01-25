package marmot.mapreduce;

import marmot.MarmotCore;
import marmot.Plan;
import marmot.exec.PlanExecution;
import marmot.exec.PlanExecutionFactory;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MultiStageMRExecutionFactory implements PlanExecutionFactory {
	private final MarmotCore m_marmot;
	
	public MultiStageMRExecutionFactory(MarmotCore marmot) {
		m_marmot = marmot;
	}
	
	@Override
	public PlanExecution create(Plan plan) {
		return MultiJobPlanExecution.create(m_marmot, plan);
	}
}
