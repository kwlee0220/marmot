package marmot.optor.geo.transform;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.operation.buffer.BufferOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.geo.GeoClientUtils;
import marmot.plan.GeomOpOptions;
import marmot.proto.optor.BufferTransformProto;
import marmot.support.DataUtils;
import marmot.support.PBSerializable;
import marmot.type.GeometryDataType;
import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BufferTransform extends SpatialRecordLevelTransform<BufferTransform>
							implements PBSerializable<BufferTransformProto> {
	private static final Logger s_logger = LoggerFactory.getLogger(BufferTransform.class);

	private GeometryDataType m_resultType;
	private double m_distance = -1;
	private String m_distanceCol = null;
	private FOption<Integer> m_segmentCount = FOption.empty();
	
	private int m_distColIdx = -1;
	
	private BufferTransform(String geomCol, double distance, FOption<Integer> segCount,
							GeomOpOptions opts) {
		super(geomCol, opts);
		Preconditions.checkArgument(Double.compare(distance, 0d) > 0,
									"invalid buffer distance: dist=" + distance);

		m_distance = distance;
		m_segmentCount = segCount;
		setLogger(s_logger);
	}
	
	private BufferTransform(String geomCol, String distCol, FOption<Integer> segCount,
							GeomOpOptions opts) {
		super(geomCol, opts);
		Utilities.checkNotNullArgument(distCol, "distance column is null");

		m_distanceCol = distCol;
		m_segmentCount = segCount;
		setLogger(s_logger);
	}

	@Override
	protected GeometryDataType initialize(GeometryDataType inGeomType, RecordSchema inputSchema) {
		if ( m_distanceCol != null ) {
			Column distCol = inputSchema.findColumn(m_distanceCol).getOrNull();
			if ( distCol == null ) {
				String msg = String.format("invalid distance column: name=%s, schema=%s",
											m_distanceCol, inputSchema);
				throw new IllegalArgumentException(msg);
			}
			switch ( distCol.type().getTypeCode() ) {
				case DOUBLE: case FLOAT: case INT: case LONG: case BYTE: case SHORT:
					m_distColIdx = distCol.ordinal();
					break;
				default:
					String msg = String.format("invalid distance column: name=%s, type=%s",
							m_distanceCol, distCol.type());
					throw new IllegalArgumentException(msg);
			}
		}
		
		switch ( inGeomType.getTypeCode() ) {
			case POINT:
			case POLYGON:
			case LINESTRING:
				m_resultType = GeometryDataType.POLYGON;
				break;
			default:
				m_resultType = GeometryDataType.MULTI_POLYGON;
				break;
		}
		
		return m_resultType;
	}

	@Override
	protected GeometryDataType initialize(GeometryDataType inGeomType) {
		throw new AssertionError("Should not be called");
	}

	@Override
	protected Geometry transform(Geometry geom, Record inputRecord) {
		double dist = m_distance;
		if ( dist < 0 ) {
			dist = DataUtils.asDouble(inputRecord.get(m_distColIdx));
		}

		Geometry buffered = m_segmentCount.isPresent()
							? BufferOp.bufferOp(geom, dist, m_segmentCount.get())
							: geom.buffer(dist);
		return GeoClientUtils.cast(buffered, m_resultType);
	}
	
	@Override
	protected Geometry transform(Geometry geom) {
		throw new AssertionError("Should not be called");
	}
	
	@Override
	public String toString() {
		return String.format("buffer_transform[%s:%.1f]", getInputGeometryColumn(),
															m_distance);
	}

	public static BufferTransform fromProto(BufferTransformProto proto) {
		FOption<Integer> nsegs = FOption.empty();
		switch (  proto.getOptionalSegmentCountCase() ) {
			case SEGMENT_COUNT:
				nsegs = FOption.of(proto.getSegmentCount());
				break;
			default:
		}
		GeomOpOptions opts = GeomOpOptions.fromProto(proto.getOptions());
		
		switch ( proto.getOneofDistanceInfoCase() ) {
			case DISTANCE:
				return new BufferTransform(proto.getGeometryColumn(), proto.getDistance(),
											nsegs, opts);
			case DISTANCE_COLUMN:
				return new BufferTransform(proto.getGeometryColumn(), proto.getDistanceColumn(),
											nsegs, opts);
			default:
				throw new AssertionError();
		}
	}

	@Override
	public BufferTransformProto toProto() {
		BufferTransformProto.Builder builder = BufferTransformProto.newBuilder()
												.setGeometryColumn(getInputGeometryColumn())
												.setOptions(m_options.toProto());
		if ( m_distance >= 0 ) {
			builder.setDistance(m_distance);
		}
		else {
			builder.setDistanceColumn(m_distanceCol);
		}
		m_segmentCount.ifPresent(builder::setSegmentCount);
		
		return builder.build();
	}
}
