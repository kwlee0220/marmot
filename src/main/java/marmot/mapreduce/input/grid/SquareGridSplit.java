package marmot.mapreduce.input.grid;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.locationtech.jts.geom.Envelope;

import marmot.io.serializer.MarmotSerializers;
import utils.Size2d;
import utils.Size2i;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SquareGridSplit extends InputSplit implements Writable {
	private static final String[] EMPTY_HOSTS = new String[0];
	
	private Envelope m_mbr;
	private int m_id;
	private Size2i m_partDim;
	private Size2d m_cellSize;
	
	public SquareGridSplit() { }
	
	public SquareGridSplit(Envelope mbr, int id, Size2i partDim, Size2d cellSize) {
		m_mbr = mbr;
		m_id = id;
		m_partDim = partDim;
		m_cellSize = cellSize;
	}
	
	Envelope getUniverse() {
		return m_mbr;
	}
	
	int getPartitionId() {
		return m_id;
	}
	
	Size2i getPartionDimension() {
		return m_partDim;
	}
	
	Size2d getCellSize() {
		return m_cellSize;
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return Math.round((m_mbr.getWidth()/m_partDim.getWidth())
							* (m_mbr.getHeight()/m_partDim.getHeight()));
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return EMPTY_HOSTS;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		m_mbr = MarmotSerializers.ENVELOPE.deserialize(in);
		m_id = in.readInt();
		m_partDim = new Size2i(in.readInt(), in.readInt());
		m_cellSize = new Size2d(in.readDouble(), in.readDouble());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		MarmotSerializers.ENVELOPE.serialize(m_mbr, out);
		out.writeInt(m_id);
		out.writeInt(m_partDim.getWidth());
		out.writeInt(m_partDim.getHeight());
		out.writeDouble(m_cellSize.getWidth());
		out.writeDouble(m_cellSize.getHeight());
	}
	
	@Override
	public String toString() {
		int row = (int)(m_id / m_partDim.getWidth());
		int col = (int)(m_id % m_partDim.getHeight());
		return String.format("(%d,%d): (%dx%d)",
							col, row, m_partDim.getWidth(), m_partDim.getHeight());
	}
}