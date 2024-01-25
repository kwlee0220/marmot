package marmot.mapreduce.input.hashjoin;

import marmot.RecordSchema;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface MarmotRecordReader {
	public RecordSchema getRecordSchema();
}
