package marmot.mapreduce.input.jdbc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class JdbcTableSplit extends InputSplit implements Writable {
	private static final String[] EMPTY = new String[0];
	
	long m_start;
	long m_length;	// -1 for null

	// just for 'Writable'
	public JdbcTableSplit() { }
	
	JdbcTableSplit(long start, long length) {
		m_start = start;
		m_length = length;
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return (m_length > 0) ? m_length : -m_length;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return EMPTY;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		m_start = in.readLong();
		m_length = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(m_start);
		out.writeLong(m_length);
	}
	
	@Override
	public String toString() {
		long end = (m_length > 0) ? m_start + m_length -1 : -1;
		return String.format("table_split[%d->%s]", m_start, (end > 0) ? "" + end : "");
	}
}