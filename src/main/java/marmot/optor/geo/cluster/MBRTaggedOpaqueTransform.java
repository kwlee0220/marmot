package marmot.optor.geo.cluster;

import javax.annotation.Nullable;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.CoordinateTransform;
import marmot.io.RecordWritable;
import marmot.optor.support.RecordLevelTransform;
import marmot.proto.optor.MBRTaggedOpaqueTransformProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;
import marmot.type.DataType;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MBRTaggedOpaqueTransform extends RecordLevelTransform
									implements PBSerializable<MBRTaggedOpaqueTransformProto> {
	public static final RecordSchema SCHEMA = RecordSchema.builder()
															.addColumn("mbr", DataType.ENVELOPE)
															.addColumn("length", DataType.INT)
															.build();
	private final GeometryColumnInfo m_gcInfo;
	@Nullable private final Envelope m_validBounds;
	private final CoordinateTransform m_trans;
	
	private int m_geomColIdx = -1;
	
	public MBRTaggedOpaqueTransform(GeometryColumnInfo gcInfo, FOption<Envelope> validBounds) {
		m_gcInfo = gcInfo;
		m_validBounds = validBounds.getOrNull();
		
		m_trans = CoordinateTransform.getTransformToWgs84(gcInfo.srid());
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		m_geomColIdx = inputSchema.getColumn(m_gcInfo.name()).ordinal();
		
		setInitialized(marmot, inputSchema, SCHEMA);
	}

	@Override
	public boolean transform(Record input, Record output) {
		Geometry geom = input.getGeometry(m_geomColIdx);
		if ( geom == null || geom.isEmpty() ) {
			return false;
		}
		
		Envelope envl = geom.getEnvelopeInternal();
		if ( m_validBounds != null ) {
			if ( !m_validBounds.intersects(envl) ) {
				return false;
			}
		}
		
		Envelope envl84 = (m_trans != null) ? m_trans.transform(envl) : envl;
		if ( envl84.getMaxY() > 85 || envl84.getMinY() < -85
			|| envl84.getMaxX() > 180 || envl84.getMinX() < -180 ) {
			return false;
		}
		
		output.set(0, envl84);
		output.set(1, RecordWritable.from(input).toBytes().length);
		
		return true;
	}
	
	@Override
	public String toString() {
		return String.format("TagQuadKeyMBR: geom_col=%s", m_gcInfo);
	}
	
	public static MBRTaggedOpaqueTransform fromProto(MBRTaggedOpaqueTransformProto proto) {
		GeometryColumnInfo info = GeometryColumnInfo.fromProto(proto.getGeometryColumn());
		FOption<Envelope> validBounds = FOption.empty();
		switch ( proto.getOptionalValidBoundsCase() ) {
			case VALID_BOUNDS:
				validBounds = FOption.of(PBUtils.fromProto(proto.getValidBounds()));
				break;
			default:
		}
		
		return new MBRTaggedOpaqueTransform(info, validBounds);
	}

	@Override
	public MBRTaggedOpaqueTransformProto toProto() {
		MBRTaggedOpaqueTransformProto.Builder builder = MBRTaggedOpaqueTransformProto.newBuilder()
															.setGeometryColumn(m_gcInfo.toProto());
		if ( m_validBounds != null ) {
			builder = builder.setValidBounds(PBUtils.toProto(m_validBounds));
		}
		return builder.build();
	}
}