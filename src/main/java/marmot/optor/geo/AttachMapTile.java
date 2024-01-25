package marmot.optor.geo;

import java.util.List;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import com.google.common.collect.Lists;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.geo.GeoClientUtils;
import marmot.geo.MapTileConverter;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.rset.AbstractRecordSet;
import marmot.support.DefaultRecord;
import marmot.type.DataType;
import marmot.type.MapTile;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AttachMapTile extends AbstractRecordSetFunction {
	private static final String DEFAULT_GEOM_COLUMN = "the_geom";
	private static final String DEFAULT_TILE_COLUMN = "tile";
	
	private final String m_srid;
	private final int m_tileZoom;
	private final double m_distance;
	private final String m_geomColName;
	private final String m_tileColName;
	
	private AttachMapTile(String srid, int tileZoom, double distance, String geomColName,
							String tileColName) {
		m_srid = srid;
		m_tileZoom = tileZoom;
		m_distance = distance;
		m_geomColName = geomColName;
		m_tileColName = tileColName;
	}
	
	public MapTileConverter getMapTileConverter() {
		return new MapTileConverter(m_srid, m_tileZoom);
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		RecordSchema outSchema = inputSchema.toBuilder()
								.addOrReplaceColumn(m_tileColName, DataType.TILE)
								.build();
		
		setInitialized(marmot, inputSchema, outSchema);
	}

	@Override
	public RecordSet apply(RecordSet input) {
		return new Attached(input);
	}
	
	@Override
	public String toString() {
		return String.format("attach_maptile[%s->%s]", m_geomColName, m_tileColName);
	}

	class Attached extends AbstractRecordSet {
		private final RecordSet m_input;
		private final MapTileConverter m_tiler;
		private final int m_geomIndex;
		private final int m_tileIndex;
		
		private List<MapTile> m_tilesRemains = Lists.newArrayList();
		private final Record m_inputRecord;
		
		public Attached( RecordSet input) {
			m_input = input;
			
			m_tiler = getMapTileConverter();
			
			RecordSchema inputSchema = input.getRecordSchema();
			Column geomCol = inputSchema.getColumn(m_geomColName);
			m_geomIndex = geomCol.ordinal();
			
			m_tileIndex = inputSchema.findColumn(m_tileColName)
									.map(Column::ordinal)
									.getOrElse(() -> inputSchema.getColumnCount());
			
			m_inputRecord = DefaultRecord.of(inputSchema);
		}
		
		@Override protected void closeInGuard() { }

		@Override
		public RecordSchema getRecordSchema() {
			return AttachMapTile.this.getRecordSchema();
		}
	
		@Override
		public boolean next(Record record) throws RecordSetException {
			while ( m_tilesRemains.size() == 0 ) {
				if ( !m_input.next(m_inputRecord) ) {
					return false;
				}
				
				Geometry geom = (Geometry)m_inputRecord.get(m_geomIndex);
				if ( Double.compare(m_distance, 0) >= 0 ) {
					Envelope envl = geom.getEnvelopeInternal();
					envl.expandBy(m_distance);
					geom = GeoClientUtils.toPolygon(envl);
				}
				m_tilesRemains = m_tiler.getContainingMapTiles(geom);
			}

			record.set(m_inputRecord);
			record.set(m_tileIndex, m_tilesRemains.remove(0));
			
			return true;
		}
	}
	
	public static Builder builder() {
		return new Builder();
	}
	public static class Builder {
		private String m_srid = "EPSG:5186";
		private String m_geomColName = DEFAULT_GEOM_COLUMN;
		private String m_tileColName = DEFAULT_TILE_COLUMN;
		private int m_zoom = -1;
		private double m_distance = -1;

		public Builder SRID(String srid) {
			m_srid = srid;
			return this;
		}
		
		public Builder tileZoom(int zoom) {
			m_zoom = zoom;
			return this;
		}
		
		public Builder geometryColumn(String colName) {
			m_geomColName = colName;
			return this;
		}
		
		public Builder tileColumn(String colName) {
			m_tileColName = colName;
			return this;
		}
		
		public Builder expandBy(double distance) {
			m_distance = distance;
			return this;
		}
		
		public AttachMapTile build() {
			return new AttachMapTile(m_srid, m_zoom, m_distance, m_geomColName, m_tileColName);
		}
	}
}