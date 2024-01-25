package marmot.mapreduce.input.fixed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import marmot.io.serializer.MarmotSerializers;
import utils.UnitUtils;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
final class FixedMapperSplit extends InputSplit implements Writable {
	private String m_jobId;
	private int m_totalSplitCount;
	
	FixedMapperSplit() { }
	FixedMapperSplit(String jobId, int totalSplitCount) {
		m_jobId = jobId;
		m_totalSplitCount = totalSplitCount;
	}
	
	String getJobId() {
		return m_jobId;
	}
	
	int getTotalSplitCount() {
		return m_totalSplitCount;
	}
	
	@Override
	public long getLength() throws IOException, InterruptedException {
		return UnitUtils.parseByteSize("512mb");
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return new String[]{};
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		m_jobId = MarmotSerializers.readString(in);
		m_totalSplitCount = MarmotSerializers.readVInt(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		MarmotSerializers.writeString(m_jobId, out);
		MarmotSerializers.writeVInt(m_totalSplitCount, out);
	}
	
	@Override
	public String toString() {
		return String.format("fixed_mapper_split[%s]", m_jobId);
	}
}