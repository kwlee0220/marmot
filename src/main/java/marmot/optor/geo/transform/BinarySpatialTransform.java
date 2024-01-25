package marmot.optor.geo.transform;

import java.util.Arrays;

import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.geo.GeoClientUtils;
import marmot.optor.support.RecordLevelTransform;
import marmot.type.DataType;
import marmot.type.DataTypes;
import marmot.type.GeometryDataType;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class BinarySpatialTransform extends RecordLevelTransform {
	private static final Logger s_logger = LoggerFactory.getLogger(BinarySpatialTransform.class);
	
	protected final String m_leftGeomColName;
	protected final String m_rightGeomColName;
	protected final String m_outputGeomCol;
	protected final FOption<GeometryDataType> m_outputGeomType;
	
	private int m_leftGeomColIdx = -1;
	private int m_rightGeomColIdx = -1;
	private int m_outputGeomColIdx = -1;
	private GeometryDataType m_outType;

	protected GeometryDataType initialize(GeometryDataType leftGeomType,
											GeometryDataType rightGeomType,
											FOption<GeometryDataType> outGeomType) {
		return outGeomType.getOrElse(leftGeomType);
	}
	abstract protected Geometry transform(Geometry left, Geometry right);
	
	protected BinarySpatialTransform(String leftGeomCol, String rightGeomCol,
									String outputGeomCol,
									FOption<GeometryDataType> outputGeomType) {
		m_leftGeomColName = leftGeomCol;
		m_rightGeomColName = rightGeomCol;
		m_outputGeomCol = outputGeomCol;
		m_outputGeomType = outputGeomType;
		
		setLogger(s_logger);
	}
	
	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		Column leftGeomCol = inputSchema.getColumn(m_leftGeomColName);
		Column rightGeomCol = inputSchema.getColumn(m_rightGeomColName);
		
		m_outType = initialize((GeometryDataType)leftGeomCol.type(),
								(GeometryDataType)rightGeomCol.type(),
								m_outputGeomType);
		RecordSchema outSchema = inputSchema.toBuilder()
										.addOrReplaceColumn(m_outputGeomCol, m_outType)
										.build();

		m_leftGeomColIdx = leftGeomCol.ordinal();
		m_rightGeomColIdx = rightGeomCol.ordinal();
		m_outputGeomColIdx = outSchema.getColumn(m_outputGeomCol).ordinal();
		
		setInitialized(marmot, inputSchema, outSchema);
	}
	
	public final GeometryDataType getOutputGeometryType() {
		return m_outType;
	}

	@Override
	public boolean transform(Record input, Record output) {
		try {
			output.set(input);
			
			Geometry left = input.getGeometry(m_leftGeomColIdx);
			Geometry right = input.getGeometry(m_rightGeomColIdx);
			Geometry transformed = transform(left, right);
			output.set(m_outputGeomColIdx, GeoClientUtils.cast(transformed, m_outType));
			
			return true;
		}
		catch ( Exception e ) {
			return false;
		}
	}
	
	public static Column parseColumnSpec(String spec) {
		String[] parts = Arrays.stream(spec.split(":"))
								.map(String::trim)
								.toArray(sz -> new String[sz]);
		DataType type = null;
		if ( parts.length == 2 ) {
			type = DataTypes.fromName(parts[1]);
		}
		else if ( parts.length > 2 ) {
			throw new IllegalArgumentException("invalid column spec: spec=" + spec);
		}
		
		return new Column(parts[0], type);
	}
	
	public static abstract class Builder<T extends Builder<T>> {
		protected String m_leftGeomCol;
		protected String m_rightGeomCol;
		protected Column m_outputGeomCol;
		
		@SuppressWarnings("unchecked")
		public T leftGeometryColumn(String colName) {
			m_leftGeomCol = colName;
			return (T)this;
		}

		@SuppressWarnings("unchecked")
		public T rightGeometryColumn(String colName) {
			m_rightGeomCol = colName;
			return (T)this;
		}

		@SuppressWarnings("unchecked")
		public T outputGeometryColumn(Column col) {
			m_outputGeomCol = col;
			return (T)this;
		}
	}
}
