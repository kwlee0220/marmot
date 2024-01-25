package marmot.geo;

import java.util.List;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.google.common.collect.Lists;

import marmot.type.MapTile;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MapTileConverter {
	private final int m_zoom;
	private final CoordinateTransform m_transform;
	
	public MapTileConverter(int zoom) {
		m_zoom = zoom;
		m_transform = null;
	}
	
	public MapTileConverter(String srid, int zoom) {
		m_zoom = zoom;
		if ( !srid.equals("EPSG:4326") ) {
			m_transform = CoordinateTransform.get(srid, "EPSG:4326");
		}
		else {
			m_transform = null;
		}
	}
	
	public MapTileConverter(CoordinateReferenceSystem crs, int zoom) {
		m_zoom = zoom;
		if ( !crs.getName().equals(CRSUtils.EPSG_4326.getName()) ) {
			m_transform = CoordinateTransform.get(crs, CRSUtils.EPSG_4326);
		}
		else {
			m_transform = null;
		}
	}
	
	public MapTile getContainingMapTile(Coordinate coord) {
		Coordinate transformed = m_transform != null ? m_transform.transform(coord) : coord;
		return MapTile.fromLonLat(transformed, m_zoom);
	}
	
	public static MapTile toMapTile(Coordinate coord, int zoom, CoordinateReferenceSystem crs) {
		return new MapTileConverter(crs, zoom).getContainingMapTile(coord);
	}
	
	public static List<MapTile> toMapTiles(Geometry geom, int zoom, CoordinateReferenceSystem crs) {
		return new MapTileConverter(crs, zoom).getContainingMapTiles(geom);
	}
	
	public List<MapTile> getContainingMapTiles(Envelope envl) {
		MapTile tlTile = getContainingMapTile(new Coordinate(envl.getMinX(), envl.getMinY()));
		MapTile brTile = getContainingMapTile(new Coordinate(envl.getMaxX(), envl.getMaxY()));
		
		return MapTile.listTilesInRange(tlTile, brTile);
	}
	
	public List<MapTile> getContainingMapTiles(Geometry geom) {
		Geometry geom4326 = m_transform != null ? m_transform.transform(geom) : geom;
		
		List<MapTile> tiles = Lists.newArrayList();
		List<MapTile> candidates = _getContainingMapTiles(geom4326.getEnvelopeInternal());
		for ( MapTile tile: candidates ) {
			if ( tile.intersects(geom4326) ) {
				tiles.add(tile);
			}
		}
		
		return tiles;
	}
	
	private List<MapTile> _getContainingMapTiles(Envelope envl4326) {
		MapTile tlTile = MapTile.fromLonLat(envl4326.getMinX(), envl4326.getMaxY(), m_zoom);
		MapTile brTile = MapTile.fromLonLat(envl4326.getMaxX(), envl4326.getMinY(), m_zoom);
		
		return MapTile.listTilesInRange(tlTile, brTile);
	}
}
