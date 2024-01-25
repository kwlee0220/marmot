package marmot.mapreduce.support;

import marmot.mapreduce.MarmotMRContext;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface MarmotReducerContext extends MarmotMRContext {
	public long getKeyInputCount();
}
