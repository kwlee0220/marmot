package marmot.optor.geo.transform;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.precision.GeometryPrecisionReducer;

import marmot.geo.GeoClientUtils;
import marmot.plan.GeomOpOptions;
import marmot.proto.optor.ReduceGeometryPrecisionProto;
import marmot.support.PBSerializable;
import marmot.type.GeometryDataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ReduceGeometryPrecision extends SpatialRecordLevelTransform<ReduceGeometryPrecision>
									implements PBSerializable<ReduceGeometryPrecisionProto> {
	private int m_precisionFactor;
	private GeometryPrecisionReducer m_reducer = null;
	
	public ReduceGeometryPrecision(String geomCol, int precisionFactor, GeomOpOptions opts) {
		super(geomCol, opts);

		m_precisionFactor = precisionFactor;
	}

	@Override
	protected GeometryDataType initialize(GeometryDataType inGeomType) {
		m_reducer = GeoClientUtils.toGeometryPrecisionReducer(m_precisionFactor);
		return inGeomType;
	}
	
	@Override
	protected Geometry transform(Geometry geom) {
		return m_reducer.reduce(geom);
	}
	
	@Override
	public String toString() {
		return String.format("reduce_precision[%s:%d]", getInputGeometryColumn(),
														m_precisionFactor);
	}
	
	public static ReduceGeometryPrecision fromProto(ReduceGeometryPrecisionProto proto) {
		GeomOpOptions opts = GeomOpOptions.fromProto(proto.getOptions());
		return new ReduceGeometryPrecision(proto.getGeometryColumn(),
											proto.getPrecisionFactor(), opts);
	}

	@Override
	public ReduceGeometryPrecisionProto toProto() {
		return ReduceGeometryPrecisionProto.newBuilder()
											.setGeometryColumn(getInputGeometryColumn())
											.setPrecisionFactor(m_precisionFactor)
											.setOptions(m_options.toProto())
											.build();
	}
}