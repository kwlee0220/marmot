package marmot.mapreduce.input.whole;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.Record;
import marmot.RecordSchema;
import marmot.io.RecordWritable;
import marmot.support.DefaultRecord;
import marmot.type.DataType;
import utils.UnitUtils;
import utils.io.IOUtils;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class WholeFileInputFormat extends CombineFileInputFormat<NullWritable, RecordWritable> {
	private static final NullWritable NULL = NullWritable.get();
	public static final RecordSchema SCHEMA = RecordSchema.builder()
															.addColumn("path", DataType.STRING)
															.addColumn("bytes", DataType.BINARY)
															.build();

	static class WholeFileRecordReader extends RecordReader<NullWritable, RecordWritable> {
		private static final Logger s_logger = LoggerFactory.getLogger(WholeFileRecordReader.class);
		
		private final Path m_path;
		private RecordWritable m_record;
		private int m_state;
		private long m_length;
		
		public WholeFileRecordReader(CombineFileSplit split, TaskAttemptContext context,
									Integer index) throws IOException {
			m_path = split.getPath(index);
		}
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
			Configuration conf = context.getConfiguration();
			
			FileSystem fs = FileSystem.get(conf);
			try ( FSDataInputStream is = fs.open(m_path) ) {
				byte[] bytes = IOUtils.toBytes(is);
				m_length = bytes.length;
				
				Record record = DefaultRecord.of(SCHEMA);
				record.set(0, m_path.toString());
				record.set(1, bytes);
				m_record = RecordWritable.from(record);
			}
			
			m_state = 0;
		}

		@Override
		public void close() throws IOException {
			s_logger.debug("done: {}", this);
		}
		
		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if ( m_state > 0 ) {
				return false;
			}
			
			if ( ++m_state == 0 ) {
				s_logger.debug("read while file: path={} nbytes={}",
								m_path, UnitUtils.toByteSizeString(m_length));
			}
			
			return true;
		}

		@Override
		public NullWritable getCurrentKey() throws IOException, InterruptedException {
			return NULL;
		}

		@Override
		public RecordWritable getCurrentValue() throws IOException, InterruptedException {
			return m_record;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return (m_state > 0) ? 1f : 0f;
		}
		
		@Override
		public String toString() {
			return String.format("load_whole_file: path=%s, length=%d", m_path.toString(), m_length);
		}
	}

    @Override
    public RecordReader<NullWritable, RecordWritable>
    createRecordReader(InputSplit split, TaskAttemptContext taskcontext) throws IOException {
        return new CombineFileRecordReader<>((CombineFileSplit)split, taskcontext,
        									WholeFileRecordReader.class);
    }
}