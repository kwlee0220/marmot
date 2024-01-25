package marmot.optor.geo.filter;

import org.locationtech.jts.geom.Geometry;

import marmot.plan.PredicateOptions;
import marmot.proto.optor.BinarySpatialIntersectsProto;
import marmot.support.PBSerializable;
import marmot.type.GeometryDataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BinarySpatialIntersects extends BinarySpatialFilter<BinarySpatialIntersects>
							implements PBSerializable<BinarySpatialIntersectsProto> {
	public BinarySpatialIntersects(String leftGeomCol, String rightGeomCol, PredicateOptions opts) {
		super(leftGeomCol, rightGeomCol, opts);
	}

	@Override
	protected void initialize(GeometryDataType leftGeomType,
								GeometryDataType rightGeomType) { }

	@Override
	protected boolean test(Geometry leftGeom, Geometry rightGeom) {
		return leftGeom.intersects(rightGeom);
	}
	
	@Override
	public String toString() {
		return String.format("intersects_binary[%s,%s]", getLeftGeometryColumn(),
														getRightGeometryColumn());
	}
	
	public static BinarySpatialIntersects fromProto(BinarySpatialIntersectsProto proto) {
		PredicateOptions opts = PredicateOptions.fromProto(proto.getOptions());
		return new BinarySpatialIntersects(proto.getLeftGeometryColumn(),
											proto.getRightGeometryColumn(), opts);
	}

	@Override
	public BinarySpatialIntersectsProto toProto() {
		return BinarySpatialIntersectsProto.newBuilder()
											.setLeftGeometryColumn(getLeftGeometryColumn())
											.setRightGeometryColumn(getRightGeometryColumn())
											.setOptions(m_options.toProto())
											.build();
	}
}
