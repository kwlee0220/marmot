package marmot.optor.geo;

import java.util.List;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;

import com.google.common.collect.Lists;

import marmot.Column;
import marmot.MarmotCore;
import marmot.MarmotServer;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.geo.GeoClientUtils;
import marmot.optor.FlatTransform;
import marmot.proto.optor.AssignSquareGridCellProto;
import marmot.support.DefaultRecord;
import marmot.support.PBSerializable;
import marmot.type.DataType;
import marmot.type.GridCell;
import utils.Size2d;
import utils.Size2i;
import utils.Utilities;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AssignSquareGridCell extends FlatTransform
									implements PBSerializable<AssignSquareGridCellProto> {
	private final String m_geomCol;
	private final SquareGrid m_grid;
	private final boolean m_assignOutside;
	
	private Envelope m_bounds;
	private Size2i m_gridSize;
	private int m_geomColIdx = -1;
	private Polygon m_universePolygon;
	private int m_outputColIdx;
	
	public AssignSquareGridCell(SquareGrid grid, String geomCol, boolean assignOutside) {
		Utilities.checkNotNullArgument(grid, "SquareGrid is null");
		Utilities.checkNotNullArgument(geomCol, "Geometry column name is null");
		
		m_grid = grid;
		m_geomCol = geomCol;
		m_assignOutside = assignOutside;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		MarmotServer server = new MarmotServer(marmot);
		
		m_bounds = m_grid.getGridBounds(server);
		Size2d cellSize = m_grid.getCellSize();
		m_gridSize = new Size2i((int)Math.ceil(m_bounds.getWidth() / cellSize.getWidth()),
								(int)Math.ceil(m_bounds.getHeight() / cellSize.getHeight()));
		
		m_geomColIdx = inputSchema.findColumn(m_geomCol)
							.map(Column::ordinal)
							.getOrThrow(() -> new IllegalArgumentException("invalid Geometry column: name=" + m_geomCol));
		
		RecordSchema outSchema = inputSchema.toBuilder()
											.addColumn("cell_geom", DataType.POLYGON)
											.addColumn("cell_pos", DataType.GRID_CELL)
											.addColumn("cell_id", DataType.LONG)
											.build();
		setInitialized(marmot, inputSchema, outSchema);
		
		m_universePolygon = GeoClientUtils.toPolygon(m_bounds);
		m_outputColIdx = inputSchema.getColumnCount();
	}

	@Override
	public RecordSet transform(Record input) {
		Geometry geom = input.getGeometry(m_geomColIdx);
		if ( geom != null && !geom.isEmpty() && m_universePolygon.intersects(geom) ) {
			FStream<Record> stream = FStream.from(findCover(input))
											.map(info -> {
												Record record = DefaultRecord.of(getRecordSchema());
												record.set(input);
												record.set(m_outputColIdx, info.m_geom);
												record.set(m_outputColIdx+1, info.m_pos);
												record.set(m_outputColIdx+2, info.m_ordinal);
												
												return record;
											});
			return RecordSet.from(getRecordSchema(), stream);
		}
		else if ( !m_assignOutside ) {
			return RecordSet.empty(getRecordSchema());
		}
		else {
			Record output = DefaultRecord.of(getRecordSchema());
			output.set(input);
			
			return RecordSet.of(output);
		}
	}
	
	@Override
	public String toString() {
		return String.format("assign_square_gridcell[%s], grid=%s", m_geomCol, m_grid);
	}

	public static AssignSquareGridCell fromProto(AssignSquareGridCellProto proto) {
		SquareGrid grid = SquareGrid.fromProto(proto.getGrid());
		String geomCol = proto.getGeometryColumn();
		boolean assignOutside = proto.getAssignOutside();
		
		return new AssignSquareGridCell(grid, geomCol, assignOutside);
	}
	
	@Override
	public AssignSquareGridCellProto toProto() {
		return AssignSquareGridCellProto.newBuilder()
										.setGeometryColumn(m_geomCol)
										.setGrid(m_grid.toProto())
										.setAssignOutside(m_assignOutside)
										.build();
	}
	
	private static class CellInfo {
		private final Geometry m_geom;
		private final GridCell m_pos;
		private final long m_ordinal;
		
		private CellInfo(Geometry geom, GridCell pos, long ordinal) {
			m_geom = geom;
			m_pos = pos;
			m_ordinal = ordinal;
		}
		
		@Override
		public String toString() {
			return String.format("%s:%d", m_pos, m_ordinal);
		}
	}
	
	private List<CellInfo> findCover(Record record) {
		Geometry geom = record.getGeometry(m_geomColIdx);
		Envelope envl = geom.getEnvelopeInternal();
		double width = m_grid.getCellSize().getWidth();
		double height = m_grid.getCellSize().getHeight();
		
		int minX = (int)Math.floor((envl.getMinX() - m_bounds.getMinX()) / width);
		int minY = (int)Math.floor((envl.getMinY() - m_bounds.getMinY()) / height);
		int maxX = (int)Math.floor((envl.getMaxX() - m_bounds.getMinX()) / width);
		int maxY = (int)Math.floor((envl.getMaxY() - m_bounds.getMinY()) / height);
		
		List<CellInfo> cover = Lists.newArrayList();
		for ( int y = minY; y <= maxY; ++y ) {
			for ( int x = minX; x <= maxX; ++x ) {
				double x1 = m_bounds.getMinX() + (x * width);
				double y1 = m_bounds.getMinY() + (y * height);
				Envelope cellEnvl = new Envelope(x1, x1 + width, y1, y1 + height);
				Polygon poly = GeoClientUtils.toPolygon(cellEnvl);
				if ( poly.intersects(geom) ) {
					long ordinal = y * (m_gridSize.getWidth()) + x;
					
					cover.add(new CellInfo(poly, new GridCell(x,y), ordinal));
				}
			}
		}
		
		return cover;
	}
}
