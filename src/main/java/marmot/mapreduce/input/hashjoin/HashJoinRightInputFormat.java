package marmot.mapreduce.input.hashjoin;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import marmot.io.RecordWritable;
import marmot.io.serializer.MarmotSerializers;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class HashJoinRightInputFormat extends HashJoinInputFormat {
	private static final String PROP_PARAMETER = "marmot.optor.equi_join.right.parameter";
	
	@Override
	public RecordReader<NullWritable, RecordWritable> createRecordReader(InputSplit split,
																TaskAttemptContext context)
		throws IOException {
		return new RightFileRecordReader();
	}
	
	private static class RightFileRecordReader extends HashJoinInputFormat.AbstractReader {
		RightFileRecordReader() {
			super(1);
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			initialize(split, context, getParameters(conf));
		}
	}
	
	private static final Parameters getParameters(Configuration conf) {
		String str = conf.get(PROP_PARAMETER);
		if ( str == null ) {
			throw new IllegalStateException("HashJoinRightInputFormat does not have parameter");
		}

		return MarmotSerializers.fromBase64String(str, Parameters::deserialize);
	}
	
	public static void setParameters(Configuration conf, Parameters param) {
		conf.set(PROP_PARAMETER, MarmotSerializers.toBase64String(param));
	}
}
