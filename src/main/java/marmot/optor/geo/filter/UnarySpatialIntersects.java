package marmot.optor.geo.filter;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;

import marmot.MarmotCore;
import marmot.plan.PredicateOptions;
import marmot.proto.GeometryProto;
import marmot.proto.optor.UnarySpatialIntersectsProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;
import marmot.type.GeometryDataType;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class UnarySpatialIntersects extends SpatialFilter<UnarySpatialIntersects>
							implements PBSerializable<UnarySpatialIntersectsProto> {
	private final String m_keyDsId;		// nullable
	private Geometry m_key;
	private PreparedGeometry m_pkey;
	
	public UnarySpatialIntersects(String geomCol, Geometry key, PredicateOptions opts) {
		super(geomCol, opts);
		Utilities.checkNotNullArgument(key, "Key geometry");

		m_keyDsId = null;
		m_key = key;
	}
	
	public UnarySpatialIntersects(String geomCol, String keyDsId, PredicateOptions opts) {
		super(geomCol, opts);
		Utilities.checkNotNullArgument(keyDsId, "Key dataset id");

		m_keyDsId = keyDsId;
		m_key = null;
	}

	@Override
	protected void initialize(MarmotCore marmot, GeometryDataType inGeomType) {
		if ( m_key == null ) {
			m_key = loadKeyGeometry(marmot, m_keyDsId);
		}
		
		m_pkey = PreparedGeometryFactory.prepare(m_key);
	}
	
	@Override
	protected boolean test(Geometry geom) {
		if ( geom == null ) {
			return false;
		}
		else {
			return m_pkey.intersects(geom);
		}
	}
	
	@Override
	public String toString() {
		return String.format("intersects_unary[%s]", getInputGeometryColumn());
	}
	
	public static UnarySpatialIntersects fromProto(UnarySpatialIntersectsProto proto) {
		PredicateOptions opts = PredicateOptions.fromProto(proto.getOptions());
		
		UnarySpatialIntersects intersects;
		switch ( proto.getEitherKeyCase() ) {
			case KEY_VALUE_DATASET:
				intersects = new UnarySpatialIntersects(proto.getGeometryColumn(),
														proto.getKeyValueDataset(), opts);
				break;
			case KEY:
				Geometry key = PBUtils.fromProto(proto.getKey());
				intersects = new UnarySpatialIntersects(proto.getGeometryColumn(), key, opts);
				break;
			default:
				throw new AssertionError();
		}
		
		return intersects;
	}

	@Override
	public UnarySpatialIntersectsProto toProto() {
		UnarySpatialIntersectsProto.Builder builder
						= UnarySpatialIntersectsProto.newBuilder()
													.setGeometryColumn(getInputGeometryColumn())
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
