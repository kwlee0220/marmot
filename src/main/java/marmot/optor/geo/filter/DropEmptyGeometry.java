package marmot.optor.geo.filter;

import org.locationtech.jts.geom.Geometry;
import org.slf4j.LoggerFactory;

import marmot.MarmotCore;
import marmot.plan.PredicateOptions;
import marmot.proto.optor.DropEmptyGeometryProto;
import marmot.support.PBSerializable;
import marmot.type.GeometryDataType;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DropEmptyGeometry extends SpatialFilter<DropEmptyGeometry>
								implements PBSerializable<DropEmptyGeometryProto> {
	public DropEmptyGeometry(String geomCol, PredicateOptions opts) {
		super(geomCol, opts);
		
		setLogger(LoggerFactory.getLogger(DropEmptyGeometry.class));
	}

	@Override
	protected void initialize(MarmotCore marmot, GeometryDataType inGeomType) { }

	@Override
	protected boolean test(Geometry geom) {
		return !(geom == null || geom.isEmpty());
	}
	
	@Override
	public String toString() {
		String opName = (getNegated()) ? "DropNonEmptyGeometry" : "DropEmptyGeometry";
		
		return String.format("%s", opName);
	}
	
	public static DropEmptyGeometry fromProto(DropEmptyGeometryProto proto) {
		PredicateOptions opts = PredicateOptions.fromProto(proto.getOptions());
		return new DropEmptyGeometry(proto.getGeometryColumn(), opts);
	}

	@Override
	public DropEmptyGeometryProto toProto() {
		return DropEmptyGeometryProto.newBuilder()
									.setGeometryColumn(getInputGeometryColumn())
									.setOptions(m_options.toProto())
									.build();
	}
}