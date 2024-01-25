package marmot.exec;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public enum PlanExecutionMode {
	DEFAULT,
	MAP_REDUCE,
	SINGLE_NODE;
	
	public static PlanExecutionMode fromOrdinal(int ordinal) {
		return values()[ordinal];
	}
}
