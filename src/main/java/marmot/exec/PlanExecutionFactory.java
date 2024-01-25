package marmot.exec;

import marmot.Plan;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface PlanExecutionFactory {
	public PlanExecution create(Plan plan);
}
