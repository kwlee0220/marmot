package marmot.optor.geo;

import org.geotools.geometry.jts.Geometries;
import org.locationtech.jts.geom.Geometry;

import com.google.common.base.Preconditions;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.geo.GeoClientUtils;
import marmot.optor.support.RecordLevelTransform;
import marmot.proto.optor.ValidateGeometryProto;
import marmot.support.PBSerializable;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ValidateGeometry extends RecordLevelTransform
								implements PBSerializable<ValidateGeometryProto> {
	private final String m_geomCol;
	
	private int m_geomColIdx = -1;
	
	public ValidateGeometry(String geomColName) {
		Preconditions.checkArgument(geomColName != null, "Geometry column is null");
		
		m_geomCol = geomColName;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		Column geomCol = inputSchema.getColumn(m_geomCol);
		m_geomColIdx = geomCol.ordinal();
		
		setInitialized(marmot, inputSchema, inputSchema);
	}

	@Override
	public boolean transform(Record input, Record output) {
		checkInitialized();
		
		Geometry geom = input.getGeometry(m_geomColIdx);
		if ( !geom.isValid() ) {
			Geometries type = Geometries.get(geom);

			geom = GeoClientUtils.makeValid(geom);
			if ( !geom.isEmpty() && geom.isValid() ) {
				geom = GeoClientUtils.cast(geom, type);
			}
			else {
				return false;
			}
		}
		
		output.set(input);
		output.set(m_geomColIdx, geom);
		
		return true;
	}
	
	@Override
	public String toString() {
		return String.format("validate_geometry[%s]", m_geomCol);
	}
	
	public static ValidateGeometry fromProto(ValidateGeometryProto proto) {
		return new ValidateGeometry(proto.getGeometryColumn());
	}

	@Override
	public ValidateGeometryProto toProto() {
		return ValidateGeometryProto.newBuilder()
									.setGeometryColumn(m_geomCol)
									.build();
	}
}