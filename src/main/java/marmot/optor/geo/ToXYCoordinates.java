package marmot.optor.geo;

import org.locationtech.jts.geom.Point;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.rset.SingleInputRecordSet;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.proto.optor.ToXYCoordinatesProto;
import marmot.support.PBSerializable;
import marmot.type.DataType;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ToXYCoordinates extends AbstractRecordSetFunction
							implements PBSerializable<ToXYCoordinatesProto> {
	private final String m_geomCol;
	private final String m_xCol;
	private final String m_yCol;
	private boolean m_keepGeomCol = false;
	
	private int m_geomColIdx = -1;
	private int m_xColIdx = -1;
	private int m_yColIdx = -1;
	
	public ToXYCoordinates(String geomCol, String xCol, String yCol) {
		Utilities.checkNotNullArgument(geomCol, "geometry column name");
		Utilities.checkNotNullArgument(xCol, "x-coordinate column name");
		Utilities.checkNotNullArgument(yCol, "y-coordinate column name");
		
		m_geomCol = geomCol;
		m_xCol = xCol;
		m_yCol = yCol;
	}
	
	public ToXYCoordinates setKeepGeomCol(boolean flag) {
		m_keepGeomCol = flag;
		return this;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		Column col = inputSchema.findColumn(m_geomCol)
						.getOrThrow(() -> {
							String details = String.format("op=%s: geometry column not found: name=%s",
															this, m_geomCol);
							throw new IllegalArgumentException(details);
						});
		if ( col.type().getTypeCode() != DataType.POINT.getTypeCode() ) {
			String details = String.format("op=%s: geometry column is not POINT name=%s",
											this, m_geomCol);
			throw new IllegalArgumentException(details);
		}
		m_geomColIdx = col.ordinal();
		
		RecordSchema.Builder builder = inputSchema.toBuilder();
		if ( !m_keepGeomCol ) {
			builder = builder.removeColumn(m_geomCol);
		}
		RecordSchema outSchema = builder.addOrReplaceColumn(m_xCol, DataType.DOUBLE)
										.addOrReplaceColumn(m_yCol, DataType.DOUBLE)
										.build();
		m_xColIdx = outSchema.getColumn(m_xCol).ordinal();
		m_yColIdx = outSchema.getColumn(m_yCol).ordinal();
		
		setInitialized(marmot, inputSchema, outSchema);
	}
	
	@Override
	public RecordSet apply(RecordSet input) {
		checkInitialized();
		
		return new Transformed(this, input);
	}
	
	@Override
	public String toString() {
		return String.format("to_xy[%s->(%s,%s)]", m_geomCol, m_xCol, m_yCol);
	}

	public static ToXYCoordinates fromProto(ToXYCoordinatesProto proto) {
		return new ToXYCoordinates(proto.getGeomColumn(), proto.getXColumn(), proto.getYColumn())
					.setKeepGeomCol(proto.getKeepGeomColumn());
	}

	@Override
	public ToXYCoordinatesProto toProto() {
		return ToXYCoordinatesProto.newBuilder()
									.setGeomColumn(m_geomCol)
									.setXColumn(m_xCol)
									.setYColumn(m_yCol)
									.setKeepGeomColumn(m_keepGeomCol)
									.build();
	}

	private static class Transformed extends SingleInputRecordSet<ToXYCoordinates> {
		private final Record m_inputRecord;
		private long m_count =0;
		private long m_nullCount =0;
		private long m_failCount =0;
		
		Transformed(ToXYCoordinates optor, RecordSet input) {
			super(optor, input);
			
			m_inputRecord = newInputRecord();
		}
		
		@Override
		public boolean next(Record record) {
			while ( m_input.next(m_inputRecord) ) {
				++m_count;
				try {
					record.set(m_inputRecord);
					
					Point pt = (Point)m_inputRecord.getGeometry(m_optor.m_geomColIdx);
					if ( pt == null || pt.isEmpty() ) {
						record.set(m_optor.m_xColIdx, null);
						record.set(m_optor.m_yColIdx, null);
						++m_nullCount;
					}
					else {
						record.set(m_optor.m_xColIdx, pt.getX());
						record.set(m_optor.m_yColIdx, pt.getY());
					}
					
					return true;
				}
				catch ( Throwable e ) {
					getLogger().warn("ignored transform failure: op=" + this, e);
					++m_failCount;
				}
			}
			
			return false;
		}
		
		@Override
		public String toString() {
			return String.format("%s: count=%d null_count=%d failed=%d",
								m_optor, m_count, m_nullCount, m_failCount);
		}
	}
}
