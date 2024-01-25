package marmot.mapreduce;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public enum ExecutionPhase {
	MAP_TIME, COMBINE_TIME, REDUCE_TIME, LOCAL;
	
	public static ExecutionPhase fromCode(int code) {
		return ExecutionPhase.values()[code];
	}
	
	public int toCode() {
		return ordinal();
	}
}
