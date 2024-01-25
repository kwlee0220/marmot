package marmot.optor.geo.transform;

import org.locationtech.jts.geom.Point;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.optor.support.RecordLevelTransform;
import marmot.proto.optor.ToGeometryPointProto;
import marmot.support.DataUtils;
import marmot.support.GeoUtils;
import marmot.support.PBSerializable;
import marmot.type.DataType;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ToGeometryPoint extends RecordLevelTransform
									implements PBSerializable<ToGeometryPointProto> {
	private final String m_xColName;
	private final String m_yColName;
	private final String m_outColName;
	
	private Column m_xCol;
	private Column m_yCol;
	private Column m_outCol;

	ToGeometryPoint(String xColName, String yColName, String outColName) {
		Utilities.checkNotNullArgument(xColName, "x-column is null");
		Utilities.checkNotNullArgument(yColName, "y-column is null");
		Utilities.checkNotNullArgument(outColName, "output column is null");
		
		m_xColName = xColName;
		m_yColName = yColName;
		m_outColName = outColName;
	}
	
	public Column getOutputGeometryColumn() {
		return m_outCol;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		m_xCol = inputSchema.findColumn(m_xColName).getOrNull();
		if ( m_xCol == null ) {
			throw new IllegalArgumentException("unknown x-column: " + m_xColName
												+ ", schema=" + inputSchema);
		}
		switch ( m_xCol.type().getTypeCode() ) {
			case DOUBLE:
			case FLOAT:
			case INT:
			case SHORT:
			case STRING:
				break;
			default:
				throw new IllegalArgumentException("invalid x-column type: name="
													+ m_xColName + ", type=" + m_xCol.type());
			
		}
		
		m_yCol = inputSchema.findColumn(m_yColName).getOrNull();
		if ( m_yCol == null ) {
			throw new IllegalArgumentException("unknown y-column: " + m_yColName + ", schema="
												+ inputSchema);
		}
		switch ( m_yCol.type().getTypeCode() ) {
			case DOUBLE:
			case FLOAT:
			case INT:
			case SHORT:
			case STRING:
				break;
			default:
				throw new IllegalArgumentException("invalid y-column type: name="
													+ m_yColName + ", type=" + m_yCol.type());
			
		}
		
		RecordSchema outSchema = inputSchema.toBuilder()
										.addColumn(m_outColName, DataType.POINT)
										.build();
		m_outCol = outSchema.getColumn(m_outColName);
		
		setInitialized(marmot, inputSchema, outSchema);
	}

	@Override
	public boolean transform(Record input, Record output) {
		output.set(input);
		
		Object xObj = getValue(input, m_xCol);
		Object yObj = getValue(input, m_yCol);
		if ( xObj != null && yObj != null ) {
			try {
				double xpos = DataUtils.asDouble(xObj);
				double ypos = DataUtils.asDouble(yObj);
				Point pt = GeoUtils.toPoint(xpos, ypos);
				output.set(m_outCol.ordinal(), pt);
			}
			catch ( Exception e ) {
				output.set(m_outCol.ordinal(), null);
				throw e;
			}
		}
		else {
			output.set(m_outCol.ordinal(), null);
		}
		
		return true;
	}
	
	@Override
	public String toString() {
		return String.format("%s: (%s,%s)->%s", getClass().getSimpleName(),
								m_xColName, m_yColName, m_outColName);
	}

	public static ToGeometryPoint fromProto(ToGeometryPointProto proto) {
		return new ToGeometryPoint(proto.getXColumn(), proto.getYColumn(),
									proto.getOutColumn());
	}

	@Override
	public ToGeometryPointProto toProto() {
		return ToGeometryPointProto.newBuilder()
									.setXColumn(m_xColName)
									.setYColumn(m_yColName)
									.setOutColumn(m_outColName)
									.build();
	}
	
	private Object getValue(Record input, Column col) {
		Object obj = input.get(col.ordinal());
		if ( obj instanceof String && ((String)obj).length() == 0 ) {
			return null;
		}
		else {
			return obj;
		}
	}
}
