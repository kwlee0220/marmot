package marmot.mapreduce.input.grid;

import java.io.DataInput;
import java.io.DataOutput;

import org.locationtech.jts.geom.Envelope;

import com.google.common.base.Preconditions;

import marmot.io.serializer.MarmotSerializable;
import marmot.io.serializer.MarmotSerializers;
import utils.Size2d;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Parameters implements MarmotSerializable {
	private static final int DEF_CELLS_PER_MAPPER = 350000;
	
	private final Envelope m_gridBounds;
	private final Size2d m_cellSize;
	private FOption<Integer> m_splitCount = FOption.empty();
	private FOption<Integer> m_cellCountPerMapper = FOption.empty();
	
	public Parameters(Envelope gridBounds, Size2d cellSize) {
		m_gridBounds = gridBounds;
		m_cellSize = cellSize;
	}
	
	public Envelope getGridBounds() {
		return m_gridBounds;
	}
	
	public Size2d getCellSize() {
		return m_cellSize;
	}
	
	public int getSplitCount() {
		return m_splitCount.getOrElse(this::calcPartitionCount);
	}
	
	public void setSplitCount(int count) {
		Preconditions.checkArgument(count > 0, "Mapper count");
		
		m_splitCount = FOption.of(count);
	}
	
	public int getCellCountPerMapper() {
		return m_cellCountPerMapper.getOrElse(DEF_CELLS_PER_MAPPER);
	}
	
	public void setCellCountPerMapper(int count) {
		Preconditions.checkArgument(count > 0, "cell-count-per-mapper");
		
		m_cellCountPerMapper = FOption.of(count);
	}
	
	public static Parameters deserialize(DataInput input) {
		Envelope gridBounds = MarmotSerializers.ENVELOPE.deserialize(input);
		Size2d cellSize = MarmotSerializers.readSize2d(input);
		Parameters params = new Parameters(gridBounds, cellSize);
		
		int mapperCount = MarmotSerializers.readVInt(input);
		if ( mapperCount > 0 ) {
			params.setSplitCount(mapperCount);
		}
		
		int cellCountPerMapper = MarmotSerializers.readInt(input);
		if ( cellCountPerMapper > 0 ) {
			params.setCellCountPerMapper(cellCountPerMapper);
		}
		
		return params;
	}

	@Override
	public void serialize(DataOutput output) {
		MarmotSerializers.ENVELOPE.serialize(m_gridBounds, output);
		MarmotSerializers.writeSize2d(m_cellSize, output);
		MarmotSerializers.writeVInt(m_splitCount.getOrElse(-1), output);
		MarmotSerializers.writeInt(m_cellCountPerMapper.getOrElse(-1), output);
	}
	
	@Override
	public String toString() {
		return String.format("(%.1f,%.1f):(%.0fkmX%.0fkm):(%.0fmX%.0fm), nmappers=%d, %dcells/mapper",
							m_gridBounds.getMinX(), m_gridBounds.getMinY(),
							m_gridBounds.getWidth()/1000, m_gridBounds.getHeight()/1000,
							m_cellSize.getWidth(), m_cellSize.getHeight(),
							getSplitCount(), getCellCountPerMapper());
	}
	
	public int calcPartitionCount() {
		long width = (long)Math.ceil(m_gridBounds.getWidth() / m_cellSize.getWidth());
		long height = (long)Math.ceil(m_gridBounds.getHeight() / m_cellSize.getHeight());
		long ncells = width * height;
		int ncellsPerMapper =  getCellCountPerMapper();

		return (int)((ncells + (ncellsPerMapper-1)) / ncellsPerMapper);
	}
}