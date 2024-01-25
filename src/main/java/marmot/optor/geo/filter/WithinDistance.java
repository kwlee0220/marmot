package marmot.optor.geo.filter;

import org.locationtech.jts.geom.Geometry;

import com.google.common.base.Preconditions;

import marmot.MarmotCore;
import marmot.plan.PredicateOptions;
import marmot.proto.GeometryProto;
import marmot.proto.optor.WithinDistanceProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;
import marmot.type.GeometryDataType;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class WithinDistance extends SpatialFilter<WithinDistance>
									implements PBSerializable<WithinDistanceProto> {
	private final String m_keyDsId;		// nullable
	private Geometry m_key;
	private final double m_distance;
	
	public WithinDistance(String geomCol, Geometry key, double distance, PredicateOptions opts) {
		super(geomCol, opts);
		
		Utilities.checkNotNullArgument(key, "key is null");
		Preconditions.checkArgument(distance >= 0);

		m_keyDsId = null;
		m_key = key;
		m_distance = distance;
	}
	
	public WithinDistance(String geomCol, String keyDsId, double distance, PredicateOptions opts) {
		super(geomCol, opts);
		Utilities.checkNotNullArgument(keyDsId, "Key dataset id");

		m_keyDsId = keyDsId;
		m_key = null;
		m_distance = distance;
	}

	@Override
	protected void initialize(MarmotCore marmot, GeometryDataType inGeomType) {
		if ( m_key == null ) {
			m_key = loadKeyGeometry(marmot, m_keyDsId);
		}
	}
	
	@Override
	protected boolean test(Geometry geom) {
		if ( geom == null ) {
			return false;
		}
		else {
			return m_key.isWithinDistance(geom, m_distance);
		}
	}
	
	public static WithinDistance fromProto(WithinDistanceProto proto) {
		PredicateOptions opts = PredicateOptions.fromProto(proto.getOptions());
		
		WithinDistance op;
		switch ( proto.getEitherKeyCase() ) {
			case KEY_VALUE_DATASET:
				op = new WithinDistance(proto.getGeometryColumn(),
										proto.getKeyValueDataset(),
										proto.getDistance(), opts);
				break;
			case KEY:
				Geometry key = PBUtils.fromProto(proto.getKey());
				op = new WithinDistance(proto.getGeometryColumn(), key,
												proto.getDistance(), opts);
				break;
			default:
				throw new AssertionError();
		}
		
		return op;
	}

	@Override
	public WithinDistanceProto toProto() {
		WithinDistanceProto.Builder builder = WithinDistanceProto.newBuilder()
													.setGeometryColumn(getInputGeometryColumn())
													.setDistance(m_distance)
													.setOptions(m_options.toProto());
		if ( m_keyDsId != null ) {
			builder.setKeyValueDataset(m_keyDsId);
		}
		else {
			GeometryProto keyProto = PBUtils.toProto(m_key);
			builder.setKey(keyProto);
		}
		
		return builder.build();
	}
}
