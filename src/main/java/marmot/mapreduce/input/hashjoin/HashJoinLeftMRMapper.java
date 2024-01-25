package marmot.mapreduce.input.hashjoin;

import org.apache.hadoop.conf.Configuration;

import marmot.RecordSchema;
import marmot.mapreduce.MarmotMRMapper;
import marmot.mapreduce.input.hashjoin.HashJoinInputFormat.Parameters;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class HashJoinLeftMRMapper extends MarmotMRMapper {
	@Override
	protected RecordSchema getInputRecordSchema(Configuration conf) {
		Parameters params = HashJoinLeftInputFormat.getParameters(conf);
		return HashJoinLeftInputFormat.calcOutputRecordSchema(params);
	}
}
