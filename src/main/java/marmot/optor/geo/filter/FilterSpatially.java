package marmot.optor.geo.filter;

import java.util.Objects;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;

import marmot.MarmotCore;
import marmot.geo.GeoClientUtils;
import marmot.optor.geo.SpatialRelation;
import marmot.plan.PredicateOptions;
import marmot.proto.optor.FilterSpatiallyProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;
import marmot.type.GeometryDataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FilterSpatially extends SpatialFilter<FilterSpatially>
							implements PBSerializable<FilterSpatiallyProto> {
	private final Envelope m_keyBounds;	// nullable
	private final String m_keyDsId;		// nullable
	private Geometry m_key;
	private PreparedGeometry m_pkey;
	private final SpatialRelation m_rel;
	
	public FilterSpatially(String geomCol, SpatialRelation rel, Envelope key,
							PredicateOptions opts) {
		super(geomCol, opts);
		Objects.requireNonNull(rel, "SpatialRelation");
		Objects.requireNonNull(key, "Key bounds");

		m_rel = rel;
		m_keyBounds = key;
		m_keyDsId = null;
		m_key = null;
	}
	
	public FilterSpatially(String geomCol, SpatialRelation rel, String keyDsId,
							PredicateOptions opts) {
		super(geomCol, opts);
		Objects.requireNonNull(rel, "SpatialRelation");
		Objects.requireNonNull(keyDsId, "Key dataset id");

		m_rel = rel;
		m_keyBounds = null;
		m_keyDsId = keyDsId;
		m_key = null;
	}
	
	public FilterSpatially(String geomCol, SpatialRelation rel, Geometry key,
							PredicateOptions opts) {
		super(geomCol, opts);
		Objects.requireNonNull(rel, "SpatialRelation");
		Objects.requireNonNull(key, "Key geometry");

		m_rel = rel;
		m_keyBounds = null;
		m_keyDsId = null;
		m_key = key;
	}

	@Override
	protected void initialize(MarmotCore marmot, GeometryDataType inGeomType) {
		if ( m_keyBounds != null ) {
			m_key = GeoClientUtils.toPolygon(m_keyBounds);
		}
		else if ( m_keyDsId != null ) {
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
			switch ( m_rel.getCode() ) {
				case CODE_INTERSECTS:
					return m_pkey.intersects(geom);
				default:
					return m_rel.test(geom, m_key);
				
			}
		}
	}
	
	@Override
	public String toString() {
		return String.format("match_spatially[%s]", getInputGeometryColumn());
	}
	
	public static FilterSpatially fromProto(FilterSpatiallyProto proto) {
		SpatialRelation rel = SpatialRelation.parse(proto.getSpatialRelation());
		PredicateOptions opts = PredicateOptions.fromProto(proto.getOptions());
		
		FilterSpatially match;
		switch ( proto.getEitherKeyCase() ) {
			case KEY_DATASET:
				match = new FilterSpatially(proto.getGeometryColumn(), rel,
											proto.getKeyDataset(), opts);
				break;
			case KEY_BOUNDS:
				Envelope bounds = PBUtils.fromProto(proto.getKeyBounds());
				match = new FilterSpatially(proto.getGeometryColumn(), rel,
											bounds, opts);
				break;
			case KEY_GEOMETRY:
				Geometry key = PBUtils.fromProto(proto.getKeyGeometry());
				match = new FilterSpatially(proto.getGeometryColumn(), rel, key, opts);
				break;
			default:
				throw new AssertionError();
		}
		return match;
	}

	@Override
	public FilterSpatiallyProto toProto() {
		FilterSpatiallyProto.Builder builder
						= FilterSpatiallyProto.newBuilder()
													.setGeometryColumn(getInputGeometryColumn())
													.setSpatialRelation(m_rel.toStringExpr())
													.setOptions(m_options.toProto());
		if ( m_keyBounds != null ) {
			builder.setKeyBounds(PBUtils.toProto(m_keyBounds));
		}
		else if ( m_keyDsId != null ) {
			builder.setKeyDataset(m_keyDsId);
		}
		else {
			builder.setKeyGeometry(PBUtils.toProto(m_key));
		}
		
		return builder.build();
	}
}
