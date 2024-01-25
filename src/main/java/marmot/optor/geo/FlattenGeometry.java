package marmot.optor.geo;

import java.util.List;

import org.locationtech.jts.geom.Geometry;
import org.slf4j.LoggerFactory;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.FlatTransform;
import marmot.proto.TypeCodeProto;
import marmot.proto.optor.FlattenGeometryProto;
import marmot.support.GeoUtils;
import marmot.support.PBSerializable;
import marmot.type.DataTypes;
import marmot.type.GeometryDataType;
import marmot.type.TypeCode;
import utils.Utilities;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FlattenGeometry extends FlatTransform
								implements PBSerializable<FlattenGeometryProto> {
	private final String m_geomColName;
	private final GeometryDataType m_componentGeomType;

	private int m_geomColIdx;
	private Class<? extends Geometry> m_geomCls;

	public static FlattenGeometry flatten(String geomColName, GeometryDataType componentGeomType) {
		return new FlattenGeometry(geomColName, componentGeomType);
	}
	
	private FlattenGeometry(String geomColName, GeometryDataType componentGeomType) {
		Utilities.checkNotNullArgument(geomColName, "Geometry column");
		Utilities.checkNotNullArgument(componentGeomType, "component Geometry type");
		
		m_geomColName = geomColName;
		m_componentGeomType = componentGeomType;
		
		setLogger(LoggerFactory.getLogger(FlattenGeometry.class));
	}

	@SuppressWarnings("unchecked")
	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		m_marmot = marmot;
		
		Column geomCol = inputSchema.getColumn(m_geomColName);
		if ( !geomCol.type().isGeometryType() ) {
			throw new IllegalArgumentException("not geometry column: name=" + m_geomColName);
		}
		m_geomColIdx = geomCol.ordinal();
		m_geomCls = (Class<? extends Geometry>)m_componentGeomType.getInstanceClass();
		
		RecordSchema outSchema = inputSchema.toBuilder()
											.addOrReplaceColumn(m_geomColName,
																m_componentGeomType)
											.build();
		setInitialized(marmot, inputSchema, outSchema);
	}

	@Override
	public RecordSet transform(Record input) {
		Geometry geom = input.getGeometry(m_geomColIdx);
		List<? extends Geometry> flatteneds = GeoUtils.flatten(geom, m_geomCls);
		FStream<Record> stream = FStream.from(flatteneds)
										.map(g -> {
											Record copied = input.duplicate();
											copied.set(m_geomColIdx, g);
											return copied;
										});
		return RecordSet.from(getRecordSchema(), stream);
	}
	
	@Override
	public String toString() {
		return String.format("flatten_geometry[%s, out_type=%s]",
							m_geomColName, m_componentGeomType);
	}
	
	public static FlattenGeometry fromProto(FlattenGeometryProto proto) {
		TypeCode tc = TypeCode.valueOf(proto.getOutGeometryType().name());
		GeometryDataType outType = (GeometryDataType)DataTypes.fromTypeCode(tc);
		return new FlattenGeometry(proto.getGeometryColumn(), outType);
	}

	@Override
	public FlattenGeometryProto toProto() {
		TypeCodeProto tc = TypeCodeProto.valueOf(m_componentGeomType.getTypeCode().name());
		return FlattenGeometryProto.newBuilder()
									.setGeometryColumn(m_geomColName)
									.setOutGeometryType(tc)
									.build();
	}
}