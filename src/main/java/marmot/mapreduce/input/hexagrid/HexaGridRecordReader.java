package marmot.mapreduce.input.hexagrid;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.Record;
import marmot.RecordSet;
import marmot.io.RecordWritable;
import marmot.support.DefaultRecord;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class HexaGridRecordReader extends RecordReader<NullWritable, RecordWritable> {
	private static final Logger s_logger = LoggerFactory.getLogger(HexaGridRecordReader.class);
	
	private RecordSet m_gridRSet;
	private Record m_current;
	private Record m_next;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
		throws IOException, InterruptedException {
		HexaGridSplit gridSplit = (HexaGridSplit)split;
		m_gridRSet = gridSplit.getRecordSet();
		
		m_current = DefaultRecord.of(m_gridRSet.getRecordSchema());
		m_next = DefaultRecord.of(m_gridRSet.getRecordSchema());
		
		if ( !m_gridRSet.next(m_next) ) {
			m_next = null;
		}

		s_logger.info("open: GridPartion[{}]", m_gridRSet);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if ( m_next != null ) {
			Record temp = m_current;
			m_current = m_next;
			m_next = temp;
			
			if ( !m_gridRSet.next(m_next) ) {
				m_next = null;
			}
			
			return true;
		}
		else {
			return false;
		}
	}

	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		return NullWritable.get();
	}

	@Override
	public RecordWritable getCurrentValue() throws IOException, InterruptedException {
		return RecordWritable.from(m_current);
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0f;
	}

	@Override
	public void close() throws IOException {
		m_gridRSet.close();
	}
}