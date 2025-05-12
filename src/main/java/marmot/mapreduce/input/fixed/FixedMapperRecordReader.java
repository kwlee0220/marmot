package marmot.mapreduce.input.fixed;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.io.MarmotFileException;
import marmot.io.RecordWritable;
import marmot.mapreduce.MarmotMRContexts;
import marmot.optor.RecordSetOperatorException;
import utils.StopWatch;
import utils.Throwables;
import utils.Tuple;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class FixedMapperRecordReader extends RecordReader<NullWritable, RecordWritable> {
	private static final Logger s_logger = LoggerFactory.getLogger(FixedMapperRecordReader.class);
	private static final NullWritable NULL = NullWritable.get();
	
	private TaskAttemptContext m_context;
	private InputSplitStore m_store;
	private String m_jobId;
	private int m_totalSplitCount;
	private InputFormat<NullWritable, RecordWritable> m_format;
	
	private InputSplit m_split;
	private int m_seqNo = -1;
	private RecordReader<NullWritable,RecordWritable> m_reader;	// null이면 end-of-rset 임
	private StopWatch m_watch;
	
	@SuppressWarnings("unchecked")
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
		m_context = context;
		
		try {
			FixedMapperFileInputFormat.Parameters params
							= FixedMapperFileInputFormat.getParameters(context.getConfiguration());
			Class<?> formatClass = Class.forName(params.getInputFormatClassName());
			m_format = (InputFormat<NullWritable, RecordWritable>)formatClass.newInstance();
		}
		catch ( Exception e ) {
			throw new MarmotFileException("" + e);
		}
		
		FixedMapperSplit mapperSplit = (FixedMapperSplit)split;
		m_jobId = mapperSplit.getJobId();
		m_totalSplitCount = mapperSplit.getTotalSplitCount();
		
		try {
			m_store = InputSplitStore.open(context.getConfiguration());
			m_reader = readNextInputSplit();
		}
		catch ( Exception e ) {
			Throwables.sneakyThrow(e);
		}
	}

	@Override
	public void close() throws IOException {
		MarmotMRContexts.unset();
	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if ( m_reader == null ) {
			return false;
		}
		while ( !m_reader.nextKeyValue() ) {
			if ( m_watch != null ) {
				m_watch.stop();
				
				if ( s_logger.isInfoEnabled() ) {
					s_logger.info("done: seqno={}, split={}, elapsed={}",
									m_seqNo, m_split, m_watch.getElapsedMillisString());
				}
				
			}
			m_watch = StopWatch.start();
			
			m_reader = readNextInputSplit();
			if ( m_reader == null ) {
				return false;
			}
		}
		
		return true;
	}

	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		return NULL;
	}

	@Override
	public RecordWritable getCurrentValue() throws IOException, InterruptedException {
		return m_reader.getCurrentValue();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (float)(m_seqNo+1) / m_totalSplitCount;
	}
	
	private RecordReader<NullWritable, RecordWritable> readNextInputSplit()
		throws IOException, InterruptedException {
		try {
			FOption<Tuple<Integer,InputSplit>> osplit = m_store.poll(m_jobId);
			if ( osplit.isAbsent() ) {
				return null;
			}
			
			m_split = osplit.getUnchecked()._2;
			
			// End-Of-Split 인가 확인한다.
			if ( m_split.getLength() < 0 ) {
				s_logger.info("found: end of InputSplit task");
				return null;
			}
			
			m_seqNo = osplit.getUnchecked()._1;
			
			RecordReader<NullWritable, RecordWritable> reader
													= m_format.createRecordReader(m_split, m_context);
			reader.initialize(m_split, m_context);
			return reader;
		}
		catch ( SQLException e ) {
			throw new RecordSetOperatorException("" + e);
		}
	}
}