package marmot.mapreduce.input.fixed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.io.MarmotFileException;
import marmot.io.RecordWritable;
import marmot.io.serializer.MarmotSerializable;
import marmot.io.serializer.MarmotSerializers;
import utils.Throwables;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FixedMapperFileInputFormat extends FileInputFormat<NullWritable, RecordWritable> {
	private static final Logger s_logger = LoggerFactory.getLogger(FixedMapperFileInputFormat.class);

	@SuppressWarnings("unchecked")
	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		Configuration conf = job.getConfiguration();
		
		Parameters params = getParameters(conf);

		List<InputSplit> splits;
		try {
			Class<?> inputFormatClass = Class.forName(params.m_inputFormatClassName);
			InputFormat<NullWritable, RecordWritable> format
						= (InputFormat<NullWritable, RecordWritable>)inputFormatClass.newInstance();
			splits = format.getSplits(job);
			
			s_logger.info("populate splits: job_id={}, mapper_count={}, split_count={}",
							params.m_jobId, params.m_mapperCount, splits.size());
		}
		catch ( Exception e ) {
			throw new MarmotFileException("" + e);
		}
		
		int count = params.m_mapperCount;
		if ( params.m_mapperCount > splits.size() ) {
			s_logger.warn("The mapperCount({}) is changed to {} because it is larger than splitCount({})",
							params.m_mapperCount, splits.size(), splits.size());
			count = splits.size();
		}
		int mapperCount = count;
		

		InputSplitStore store = null;
		try {
			store = InputSplitStore.open(conf);
			store.insertInputSplit(params.m_jobId, splits, mapperCount);
			
			InputSplit kjSplit = new FixedMapperSplit(params.m_jobId, splits.size());
			return FStream.range(0, mapperCount)
							.mapToObj(idx -> kjSplit)
							.toList();
		}
		catch ( Exception e ) {
			if ( store != null ) {
				try {
					store.removeJob(params.m_jobId);
				}
				catch ( SQLException e1 ) {
					s_logger.warn("fails to remove job splits", e1);
				}
			}
			
			throw Throwables.toRuntimeException(Throwables.unwrapThrowable(e));
		}
	}

	@Override
	public RecordReader<NullWritable, RecordWritable> createRecordReader(InputSplit split,
																		TaskAttemptContext context)
		throws IOException, InterruptedException {
		return new FixedMapperRecordReader();
	}

	private static final String PROP_PARAMETER = "marmot.mapreduce.input.fixed_mapper.parameter";
	public static class Parameters implements MarmotSerializable {
		private final String m_jobId;
		private final int m_mapperCount;
		private final String m_inputFormatClassName;
		
		public Parameters(String jobId, int mapperCount, String inputFormatClassName) {
			m_jobId = jobId;
			m_mapperCount = mapperCount;
			m_inputFormatClassName = inputFormatClassName;
		}
		
		public int getMapperCount() {
			return m_mapperCount;
		}
		
		public String getInputFormatClassName() {
			return m_inputFormatClassName;
		}
		
		public static Parameters deserialize(DataInput input) {
			String jobId = MarmotSerializers.readString(input);
			int mapperCount = MarmotSerializers.readVInt(input);
			String ifClsName = MarmotSerializers.readString(input);
			
			return new Parameters(jobId, mapperCount, ifClsName);
		}

		@Override
		public void serialize(DataOutput output) {
			MarmotSerializers.writeString(m_jobId, output);
			MarmotSerializers.writeVInt(m_mapperCount, output);
			MarmotSerializers.writeString(m_inputFormatClassName, output);
		}
	}
	
	public static final Parameters getParameters(Configuration conf) {
		String str = conf.get(PROP_PARAMETER);
		if ( str == null ) {
			throw new IllegalStateException(FixedMapperFileInputFormat.class
											+ " does not have its parameter");
		}

		return MarmotSerializers.fromBase64String(str, Parameters::deserialize);
	}
	
	public static void setParameters(Configuration conf, Parameters params) {
		conf.set(PROP_PARAMETER, MarmotSerializers.toBase64String(params));
	}
}
