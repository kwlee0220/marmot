package marmot.geo;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import marmot.type.GridCell;
import utils.Size2d;
import utils.Size2i;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SquareGridCellAssigner {
	private final Envelope m_bounds;
	private final Size2i m_resolution;
	private final Size2d m_unit;
	
	public SquareGridCellAssigner(Envelope bounds, Size2i resolution) {
		m_bounds = bounds;
		m_resolution = resolution;
		m_unit = GeoClientUtils.divide(m_bounds, m_resolution);
	}
	
	public Assignment assign(Point pt) {
		GridCell pos = toGridCell(pt);
		long id = toId(pos);
		
		double minX = m_bounds.getMinX() + pos.getX() * m_unit.getWidth();
		double minY = m_bounds.getMinY() + pos.getY() * m_unit.getHeight();
		Envelope envl = new Envelope(minX, minX + m_unit.getWidth(),
									minY, minY + m_unit.getHeight());
		
		return new Assignment(pos, id, envl);
	}
	
	public static class Assignment {
		private final GridCell m_cellPos;
		private final long m_cellId;
		private final Envelope m_envl;
		private final Polygon m_cellPoly;
		
		private Assignment(GridCell pos, long id, Envelope envl) {
			m_cellPos = pos;
			m_cellId = id;
			m_envl = envl;
			m_cellPoly = GeoClientUtils.toPolygon(m_envl);
		}
		
		public GridCell getCellPos() {
			return m_cellPos;
		}
		
		public long getCellId() {
			return m_cellId;
		}
		
		public Envelope getCellEnvelope() {
			return m_envl;
		}
		
		public Polygon getCellPolygon() {
			return m_cellPoly;
		}
	}
	
	private GridCell toGridCell(Point pt) {
		int x = (int)Math.floor((pt.getX() - m_bounds.getMinX()) / m_unit.getWidth());
		int y = (int)Math.floor((pt.getY() - m_bounds.getMinY()) / m_unit.getHeight());
		
		return new GridCell(x, y);
	}
	
	private long toId(GridCell pos) {
		return (pos.getY() * m_resolution.getWidth()) + pos.getX();
	}
}
