package marmot.optor.geo.transform;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.plan.GeomOpOptions;
import marmot.proto.optor.CentroidTransformProto;
import marmot.support.PBSerializable;
import marmot.type.GeometryDataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CentroidTransform extends SpatialRecordLevelTransform<CentroidTransform>
								implements PBSerializable<CentroidTransformProto> {
	private static final Logger s_logger = LoggerFactory.getLogger(CentroidTransform.class);
	
	private final boolean m_inside;
	
	private CentroidTransform(String inputGeomCol, boolean inside, GeomOpOptions opts) {
		super(inputGeomCol, opts);

		m_inside = inside;
		setLogger(s_logger);
	}

	@Override
	protected GeometryDataType initialize(GeometryDataType inGeomType) {
		return GeometryDataType.POINT;
	}
	
	@Override
	protected Point transform(Geometry geom) {
		if ( geom == null ) {
			return null;
		}
		else if ( m_inside ) {
			return geom.getInteriorPoint();
		}
		else {
			return geom.getCentroid();
		}
	}
	
	@Override
	public String toString() {
		return String.format("%s: geom=%s", getClass().getSimpleName(), getInputGeometryColumn());
	}

	public static CentroidTransform fromProto(CentroidTransformProto proto) {
		GeomOpOptions opts = GeomOpOptions.fromProto(proto.getOptions());
		return new CentroidTransform(proto.getGeometryColumn(), proto.getInside(), opts);
	}

	@Override
	public CentroidTransformProto toProto() {
		return CentroidTransformProto.newBuilder()
										.setGeometryColumn(getInputGeometryColumn())
										.setInside(m_inside)
										.setOptions(m_options.toProto())
										.build();
	}
}
