package marmot.optor.geo.filter;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import marmot.MarmotCore;
import marmot.plan.PredicateOptions;
import marmot.proto.optor.UnaryEnvelopeIntersectsProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;
import marmot.type.GeometryDataType;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class UnaryEnvelopeIntersects extends SpatialFilter<UnaryEnvelopeIntersects>
									implements PBSerializable<UnaryEnvelopeIntersectsProto> {
	private final Envelope m_key;
	
	public UnaryEnvelopeIntersects(String geomCol, Envelope key, PredicateOptions opts) {
		super(geomCol, opts);
		Utilities.checkNotNullArgument(key, "Key Envelope");

		m_key = key;
	}

	@Override
	protected void initialize(MarmotCore marmot, GeometryDataType inGeomType) {
	}
	
	@Override
	protected boolean test(Geometry geom) {
		if ( geom == null ) {
			return false;
		}
		else {
			return m_key.intersects(geom.getEnvelopeInternal());
		}
	}
	
	@Override
	public String toString() {
		return String.format("%s: col=%s, range=%s", getClass().getSimpleName(),
								getInputGeometryColumn(), m_key);
	}
	
	public static UnaryEnvelopeIntersects fromProto(UnaryEnvelopeIntersectsProto proto) {
		PredicateOptions opts = PredicateOptions.fromProto(proto.getOptions());
		Envelope key = PBUtils.fromProto(proto.getKey());
		return new UnaryEnvelopeIntersects(proto.getGeometryColumn(), key, opts);
	}

	@Override
	public UnaryEnvelopeIntersectsProto toProto() {
		return UnaryEnvelopeIntersectsProto.newBuilder()
											.setGeometryColumn(getInputGeometryColumn())
											.setKey(PBUtils.toProto(m_key))
											.build();
	}
}
