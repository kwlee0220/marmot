package marmot.optor.geo.transform;

import org.locationtech.jts.geom.Geometry;

import marmot.optor.support.SafeIntersection;
import marmot.proto.TypeCodeProto;
import marmot.proto.optor.BinarySpatialIntersectionProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;
import marmot.type.DataTypes;
import marmot.type.GeometryDataType;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BinarySpatialIntersection extends BinarySpatialTransform
							implements PBSerializable<BinarySpatialIntersectionProto> {
	private static final int DEFAULT_REDUCE_FACTOR = 2;
	
	private SafeIntersection m_op;
	
	public BinarySpatialIntersection(String leftGeomCol, String rightGeomCol,
									String outGeomCol, FOption<GeometryDataType> outGeomType) {
		super(leftGeomCol, rightGeomCol, outGeomCol, outGeomType);
	}

	@Override
	protected GeometryDataType initialize(GeometryDataType leftGeomType,
											GeometryDataType rightGeomType,
											FOption<GeometryDataType> outGeomType) {
		GeometryDataType outType = outGeomType.getOrElse(leftGeomType);
		m_op = new SafeIntersection(outType.toGeometries())
					.setReduceFactor(DEFAULT_REDUCE_FACTOR);
		
		return outType;
	}
	
	@Override
	protected Geometry transform(Geometry left, Geometry right) {
		return m_op.apply(left, right);
	}
	
	@Override
	public String toString() {
		return String.format("spatial_intersection: (%s, %s)->%s, type=%s",
							m_leftGeomColName, m_rightGeomColName, m_outputGeomCol,
							getOutputGeometryType());
	}
	
	public static BinarySpatialIntersection fromProto(BinarySpatialIntersectionProto proto) {
		FOption<TypeCodeProto> tcp = PBUtils.getOptionField(proto, "out_geometry_type");
		FOption<GeometryDataType> outType = tcp.map(p ->
												(GeometryDataType)DataTypes.fromName(p.name()));
		return new BinarySpatialIntersection(proto.getLeftGeometryColumn(),
											proto.getRightGeometryColumn(),
											proto.getOutGeometryColumn(), outType);
	}

	@Override
	public BinarySpatialIntersectionProto toProto() {
		BinarySpatialIntersectionProto.Builder builder
			= BinarySpatialIntersectionProto.newBuilder()
											.setLeftGeometryColumn(m_leftGeomColName)
											.setRightGeometryColumn(m_rightGeomColName)
											.setOutGeometryColumn(m_outputGeomCol);
		m_outputGeomType.ifPresent(t -> TypeCodeProto.valueOf(t.getName()));
		return builder.build();
	}
}
