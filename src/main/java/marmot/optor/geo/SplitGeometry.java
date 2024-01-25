package marmot.optor.geo;

import java.util.List;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.geo.GeoClientUtils;
import marmot.mapreduce.MarmotMRContexts;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.proto.optor.SplitGeometryProto;
import marmot.rset.AbstractRecordSet;
import marmot.support.DefaultRecord;
import marmot.support.PBSerializable;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SplitGeometry extends AbstractRecordSetFunction
							implements PBSerializable<SplitGeometryProto> {
	private static final Logger s_logger = LoggerFactory.getLogger(SplitGeometry.class);
	
	private static final int MAX_NPOINTS = 512;
	private static final long MAX_AREA = 2000 * 2000;
	private static final int MAX_REPORT_NPOINTS = 10000;
	private static final int MAX_REPORT_AREA = 5000*5000;
	
	private final String m_geomCol;
	
	public SplitGeometry(String geomCol) {
		m_geomCol = geomCol;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		setInitialized(marmot, inputSchema, inputSchema);
	}

	@Override
	public RecordSet apply(RecordSet input) {
		checkInitialized();
		
		return new Splitteds(input);
	}

	public static SplitGeometry fromProto(SplitGeometryProto proto) {
		return new SplitGeometry(proto.getGeometryColumn());
	}

	@Override
	public SplitGeometryProto toProto() {
		return SplitGeometryProto.newBuilder()
							.setGeometryColumn(m_geomCol)
							.build();
	}
	
	private class Splitteds extends AbstractRecordSet {
		private final RecordSet m_input;
		private final int m_geomColIdx;
		private final List<Geometry> m_remains = Lists.newArrayList();
		private Record m_base;

		protected Splitteds(RecordSet input) {
			m_input = input;

			RecordSchema inputSchema = input.getRecordSchema();
			Column col = inputSchema.findColumn(m_geomCol).getOrNull();
			if ( col == null ) {
				throw new IllegalArgumentException("invalid Geometry column: name=" + m_geomCol);
			}
			m_geomColIdx = col.ordinal();
			m_base = DefaultRecord.of(inputSchema);
		}
		
		@Override
		protected void closeInGuard() {
			m_input.closeQuietly();
		}

		@Override
		public RecordSchema getRecordSchema() {
			return SplitGeometry.this.getRecordSchema();
		}
		
		public boolean next(Record output) {
			while ( true ) {
				while ( !m_remains.isEmpty() ) {
					Geometry geom = m_remains.remove(m_remains.size()-1);
					int npoints = geom.getNumPoints();
					double area = geom.getArea();
					if ( npoints > MAX_REPORT_NPOINTS || area > MAX_REPORT_AREA ) {
						s_logger.info(String.format("split: area=%11.0f npoints=%d",
											geom.getArea(), geom.getNumPoints()));
					}
					if ( npoints > MAX_NPOINTS || area > MAX_AREA ) {
						split(geom, 2, 2);
					}
					else {
						output.set(m_base);
						output.set(m_geomColIdx, geom);
						
						return true;
					}
				}
				
				if ( !m_input.next(m_base) ) {
					return false;
				}
				m_remains.add(m_base.getGeometry(m_geomColIdx));
				m_base.set(m_geomColIdx, null);
			}
		}
		
		private void split(Geometry geom, int nrows, int ncols) {
			Envelope mbr = geom.getEnvelopeInternal();
			
			double minX = mbr.getMinX();
			double minY = mbr.getMinY();
			double width = mbr.getWidth() / ncols;
			double height = mbr.getHeight() / nrows;
			
			for ( int x = 0; x < ncols; ++x ) {
				for ( int y =0; y < nrows; ++y ) {
					Coordinate tl = new Coordinate(minX + (x*width), minY + (y*height));
					Coordinate br = new Coordinate(tl.x + width, tl.y + height);
					
					StopWatch watch = StopWatch.start();
					Envelope frame = new Envelope(tl, br);
					Geometry inters = GeoClientUtils.toPolygon(frame).intersection(geom);
					watch.stop();
					
					if ( !inters.isEmpty() ) {
						MarmotMRContexts.reportProgress();
						
						List<Polygon> comps = GeoClientUtils.flatten(inters, Polygon.class);
						for ( Polygon poly: comps ) {
							int npoints = poly.getNumPoints();
							double area = poly.getArea();
							if ( npoints > MAX_REPORT_NPOINTS || area > MAX_REPORT_AREA ) {
								s_logger.info(String.format(
													"\tsplitted(%2d): area=%11.0f npoints=%8d (elapsed=%s)",
													comps.size(), area, npoints, watch.getElapsedMillisString()));
							}
							
							m_remains.add(poly);
						}
					}
				}
			}
		}
	}

	public static Builder builder() {
		return new Builder();
	}
	public static class Builder {
		private String m_geomColName;
		
		public SplitGeometry build() {
			return new SplitGeometry(m_geomColName);
		}
		
		/**
		 *  클립 조인에 참여하는 outer 레이어의 공간 컬럼 이름을 설정한다.
		 *  
		 *  @param geomCol	 공간 컬럼 이름
		 *  @return	사용 중인 빌더 객체
		 */
		public Builder usingGeometry(String geomCol) {
			m_geomColName = geomCol;
			return this;
		}
	}
}
