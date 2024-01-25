package marmot.mapreduce.input.grid;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.Record;
import marmot.io.RecordWritable;
import marmot.optor.rset.GridRecordSet;
import marmot.support.DefaultRecord;
import utils.Size2d;
import utils.Size2i;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class SquareGridRecordReader extends RecordReader<NullWritable, RecordWritable> {
	private static final Logger s_logger = LoggerFactory.getLogger(SquareGridRecordReader.class);
	
	private GridRecordSet m_gridRSet;
	private Record m_current;
	private Record m_next;
	
	private int m_length;
	private int m_idx;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
		throws IOException, InterruptedException {
		SquareGridSplit gridSplit = (SquareGridSplit)split;
		Envelope mbr = gridSplit.getUniverse();
		int partId = gridSplit.getPartitionId();
		Size2i partDim = gridSplit.getPartionDimension();
		Size2d cellSize = gridSplit.getCellSize();
		
		m_gridRSet = new GridRecordSet(mbr, partId, partDim, cellSize);
		m_current = DefaultRecord.of(m_gridRSet.getRecordSchema());
		m_next = DefaultRecord.of(m_gridRSet.getRecordSchema());
		
		if ( !m_gridRSet.next(m_next) ) {
			m_next = null;
		}
		
		m_length = m_gridRSet.getSize().getArea();
		m_idx = 1;

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
			++m_idx;
			
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
		return (float)m_idx / m_length;
	}

	@Override
	public void close() throws IOException {
		m_gridRSet.close();
	}
}