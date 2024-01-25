package marmot.optor.geo.transform;

import org.locationtech.jts.geom.Geometry;

import marmot.optor.support.SafeUnion;
import marmot.proto.optor.BinarySpatialUnionProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;
import marmot.type.DataTypes;
import marmot.type.GeometryDataType;
import marmot.type.TypeCode;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BinarySpatialUnion extends BinarySpatialTransform
								implements PBSerializable<BinarySpatialUnionProto> {
	private SafeUnion m_op;
	
	public BinarySpatialUnion(String leftGeomCol, String rightGeomCol,
									String outGeomCol, FOption<GeometryDataType> outGeomType) {
		super(leftGeomCol, rightGeomCol, outGeomCol, outGeomType);
	}

	@Override
	protected GeometryDataType initialize(GeometryDataType leftGeomType,
											GeometryDataType rightGeomType,
											FOption<GeometryDataType> outGeomType) {
		GeometryDataType outType = outGeomType.getOrElse(leftGeomType);
		m_op = new SafeUnion(outType.toGeometries());
		
		return outType;
	}
	
	@Override
	protected Geometry transform(Geometry left, Geometry right) {
		return m_op.apply(left, right);
	}
	
	@Override
	public String toString() {
		return String.format("spatial_union: (%s, %s)->%s, type=%s",
							m_leftGeomColName, m_rightGeomColName, m_outputGeomCol,
							getOutputGeometryType());
	}
	
	public static BinarySpatialUnion fromProto(BinarySpatialUnionProto proto) {
		FOption<GeometryDataType> outType;
		switch ( proto.getOptionalOutGeometryTypeCase() ) {
			case OUT_GEOMETRY_TYPE:
				TypeCode tc = PBUtils.fromProto(proto.getOutGeometryType());
				GeometryDataType gdt = (GeometryDataType)DataTypes.fromTypeCode(tc);
				outType = FOption.of(gdt);
				break;
			case OPTIONALOUTGEOMETRYTYPE_NOT_SET:
				outType = FOption.empty();
				break;
			default:
				throw new AssertionError();
		}
		
		return new BinarySpatialUnion(proto.getLeftGeometryColumn(),
											proto.getRightGeometryColumn(),
											proto.getOutGeometryColumn(), outType);
	}

	@Override
	public BinarySpatialUnionProto toProto() {
		BinarySpatialUnionProto.Builder builder
								= BinarySpatialUnionProto.newBuilder()
														.setLeftGeometryColumn(m_leftGeomColName)
														.setRightGeometryColumn(m_rightGeomColName)
														.setOutGeometryColumn(m_outputGeomCol);
		m_outputGeomType.ifPresent(t -> builder.setOutGeometryType(PBUtils.toProto(t.getTypeCode())));
		return builder.build();
	}
}
