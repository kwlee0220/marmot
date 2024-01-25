package marmot.optor.rset;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import marmot.Record;
import marmot.RecordSchema;
import marmot.geo.GeoClientUtils;
import marmot.rset.AbstractRecordSet;
import marmot.support.Range;
import marmot.type.DataType;
import marmot.type.GridCell;
import utils.Size2d;
import utils.Size2i;
import utils.Size2l;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GridRecordSet extends AbstractRecordSet {
	private static final Logger s_logger = LoggerFactory.getLogger(GridRecordSet.class);
	
	public static final RecordSchema SCHEMA = RecordSchema.builder()
														.addColumn("the_geom", DataType.POLYGON)
														.addColumn("cell_pos", DataType.GRID_CELL)
														.addColumn("cell_id", DataType.LONG)
														.build();

	private final Coordinate m_origin;
	private final Size2l m_dim;
	private final Size2d m_cellSize;
	private final Range<Integer> m_xRange;
	private final Range<Integer> m_yRange;
	
	private int m_cellX;
	private int m_cellY;
	private long m_ordinal;

	public GridRecordSet(Envelope mbr, int partId, Size2i partDim, Size2d cellSize) {
		m_origin = new Coordinate(mbr.getMinX(), mbr.getMinY());
		m_cellSize = cellSize;

		int width = (int)Math.ceil(mbr.getWidth() / cellSize.getWidth());
		int height = (int)Math.ceil(mbr.getHeight() / cellSize.getHeight());
		m_dim = new Size2l(width, height);
		
		int partRow = partId / partDim.getWidth();
		int partCol = partId % partDim.getWidth();
		int ncols = (width + (partDim.getWidth()-1)) / partDim.getWidth();
		int nrows = (height + (partDim.getHeight()-1)) / partDim.getHeight();
		
		m_xRange = Range.closed(partCol*ncols, Math.min(width-1, (partCol+1)*ncols-1));
		m_yRange = Range.closed(partRow*nrows, Math.min(height-1, (partRow+1)*nrows-1));
		
		m_cellX = m_xRange.upperEndpoint();
		m_cellY = m_yRange.lowerEndpoint()-1;
		m_ordinal = (m_cellY * m_dim.getWidth()) + m_cellX;
		
		s_logger.info("created: {}: {}", getClass().getSimpleName(), this);
	}
	
	@Override protected void closeInGuard() { }

	@Override
	public RecordSchema getRecordSchema() {
		return SCHEMA;
	}
	
	public Size2i getSize() {
		int width = (int)(m_xRange.upperEndpoint() - m_xRange.lowerEndpoint());
		int height = (int)(m_yRange.upperEndpoint() - m_yRange.lowerEndpoint());
		return new Size2i(width, height);
	}
	
	public boolean next(Record record) {
		checkNotClosed();
		
		if ( m_xRange == null || m_yRange == null ) {
			return false;
		}
		
		if ( !m_xRange.contains(m_cellX + 1) ) {
			if ( !m_yRange.contains(m_cellY + 1) ) {
				return false;
			}
			
			m_cellX = m_xRange.lowerEndpoint();
			++m_cellY;
		}
		else {
			++m_cellX;
		}
		
		GridCell cell = GridCell.fromXY(m_cellX, m_cellY);
		Envelope cellBounds = cell.getEnvelope(m_origin, m_cellSize);
		m_ordinal = (m_cellY * m_dim.getWidth()) + m_cellX;
		
		record.set(0, GeoClientUtils.toPolygon(cellBounds));
		record.set(1, cell);
		record.set(2, m_ordinal);
		
		return true;
	}
	
	@Override
	public String toString() {
		long rangeWidth = (m_xRange != null)
						? m_xRange.upperEndpoint()-m_xRange.lowerEndpoint() + 1 :-1;
		long rangeHeight = (m_yRange != null)
						? m_yRange.upperEndpoint()-m_yRange.lowerEndpoint() + 1 : -1;
		Size2l partSize = new Size2l(rangeWidth, rangeHeight);
		
		return String.format("(%s),part(%s),cell(%s): cur(%d,%d): x:%s, y:%s, ordinal=%d",
							m_dim, partSize, m_cellSize.toString(0),
							m_cellY, m_cellX, m_xRange, m_yRange, m_ordinal);
	}
	
	public static Builder builder() {
		return new Builder();
	}
	
	public static class Builder {
		private Envelope m_universe;
		private int m_partNo;
		private Size2i m_partDim;
		private Size2d m_cellSize;
		
		public GridRecordSet build() {
			Preconditions.checkState(m_universe != null, "universe has not been set");
			Preconditions.checkState(m_partDim != null, "partion dimension has not been set");
			Preconditions.checkState(m_cellSize != null, "cellSize has not been set");
			
			return new GridRecordSet(m_universe, m_partNo, m_partDim, m_cellSize);
		}
		
		public Builder universe(Envelope univ) {
			m_universe = univ;
			return this;
		}
		
		public Builder partionNumber(int no) {
			m_partNo = no;
			return this;
		}
		
		public Builder partions(Size2i partions) {
			m_partDim = partions;
			return this;
		}
		
		public Builder cellSize(Size2d cellSize) {
			m_cellSize = cellSize;
			return this;
		}
	}
}
