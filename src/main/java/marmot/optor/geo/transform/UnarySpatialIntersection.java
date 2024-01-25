package marmot.optor.geo.transform;

import org.geotools.geometry.jts.Geometries;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;

import com.google.protobuf.ByteString;

import marmot.geo.GeoClientUtils;
import marmot.optor.RecordSetOperatorException;
import marmot.optor.support.SafeIntersection;
import marmot.plan.GeomOpOptions;
import marmot.proto.optor.UnarySpatialIntersectionProto;
import marmot.support.PBSerializable;
import marmot.type.GeometryDataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class UnarySpatialIntersection extends SpatialRecordLevelTransform<UnarySpatialIntersection>
							implements PBSerializable<UnarySpatialIntersectionProto> {
	private final Geometry m_param;
	
	private SafeIntersection m_intersection = null;
	
	public UnarySpatialIntersection(String geomCol, Geometry param, GeomOpOptions opts) {
		super(geomCol, opts);
		
		m_param = param;
	}

	@Override
	protected GeometryDataType initialize(GeometryDataType inGeomType) {
		Geometries dstType = inGeomType.toGeometries();
		m_intersection = new SafeIntersection(dstType);
//		m_precisionFactor.forEach(factor -> m_intersection.setReduceFactor(factor));
		
		return inGeomType;
	}
	
	@Override
	protected Geometry transform(Geometry geom) {
		return m_intersection.apply(geom, m_param);
	}
	
	public static UnarySpatialIntersection fromProto(UnarySpatialIntersectionProto proto) {
		try {
			Geometry param = GeoClientUtils.fromWKB(proto.getKey().toByteArray());
			GeomOpOptions opts = GeomOpOptions.fromProto(proto.getOptions());
			return new UnarySpatialIntersection(proto.getGeometryColumn(), param, opts);
		}
		catch ( ParseException e ) {
			throw new RecordSetOperatorException("fails to deserialize operator: "
								+ UnarySpatialIntersection.class.getSimpleName(), e);
		}
	}

	@Override
	public UnarySpatialIntersectionProto toProto() {
		byte[] wkb = GeoClientUtils.toWKB(m_param);
		return UnarySpatialIntersectionProto.newBuilder()
											.setGeometryColumn(getInputGeometryColumn())
											.setKey(ByteString.copyFrom(wkb))
											.setOptions(m_options.toProto())
											.build();
	}
}
