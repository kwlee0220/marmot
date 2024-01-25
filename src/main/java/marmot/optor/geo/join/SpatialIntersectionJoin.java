package marmot.optor.geo.join;

import java.util.Iterator;
import java.util.Map;

import org.geotools.geometry.jts.Geometries;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPolygon;

import com.google.common.collect.Maps;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.RecordSetOperatorException;
import marmot.optor.support.JoinUtils;
import marmot.optor.support.SafeIntersection;
import marmot.optor.support.colexpr.ColumnSelector;
import marmot.plan.SpatialJoinOptions;
import marmot.proto.optor.SpatialIntersectionJoinProto;
import marmot.rset.AbstractRecordSet;
import marmot.support.GeoUtils;
import marmot.support.PBSerializable;
import marmot.type.GeometryDataType;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialIntersectionJoin extends NestedLoopSpatialJoin<SpatialIntersectionJoin>
									implements PBSerializable<SpatialIntersectionJoinProto> {
	private ColumnSelector m_selector;
	private SafeIntersection m_intersection;
	
	public SpatialIntersectionJoin(String inputGeomCol, String paramDsId, SpatialJoinOptions opts) {
		super(inputGeomCol, paramDsId, opts);
	}

	@Override
	protected RecordSchema initialize(MarmotCore marmot, RecordSchema outerSchema,
										RecordSchema innerSchema) {
		Column geomCol = getOuterGeomColumn();

		m_intersection = new SafeIntersection().setReduceFactor(1);
		Geometries dstType = ((GeometryDataType)geomCol.type()).toGeometries();
		m_intersection.setResultType(dstType);
		
		try {
			m_selector = JoinUtils.createJoinColumnSelector(outerSchema, innerSchema,
															m_options.outputColumns());
			return m_selector.getRecordSchema();
		}
		catch ( Exception e ) {
 			throw new RecordSetOperatorException(String.format("op=%s, cause=%s", this, e));
		}
	}

	@Override
	protected RecordSet doInnerLoop(NestedLoopMatch match) {
		return new MatchRecordSet(match);
	}
	
	@Override
	public String toString() {
		return String.format("intersection_join[%s<->%s]", getOuterGeomColumn(),
								getParamDataSetId());
	}

	public static SpatialIntersectionJoin fromProto(SpatialIntersectionJoinProto proto) {
		SpatialJoinOptions opts = SpatialJoinOptions.fromProto(proto.getOptions());
		return new SpatialIntersectionJoin(proto.getGeomColumn(), proto.getParamDataset(), opts);
	}

	@Override
	public SpatialIntersectionJoinProto toProto() {
		return SpatialIntersectionJoinProto.newBuilder()
											.setGeomColumn(getOuterGeomColumnName())
											.setParamDataset(getParamDataSetId())
											.setOptions(m_options.toProto())
											.build();
	}
	
	private class MatchRecordSet extends AbstractRecordSet {
		private final Geometry m_outGeom;
		private Iterator<Record> m_inners;
		private final Map<String,Record> m_binding = Maps.newHashMap();
		
		public MatchRecordSet(NestedLoopMatch match) {
			m_outGeom = getOuterGeometry(match.getOuterRecord());
			m_binding.put("left", match.getOuterRecord());
			
			m_inners = match.getInnerRecords().iterator();
		}

		@Override
		protected void closeInGuard() {
			m_inners = null;
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_selector.getRecordSchema();
		}
		
		public boolean next(Record record) {
			checkNotClosed();

			while ( m_inners.hasNext() ) {
				Record inner = m_inners.next();
				Geometry inGeom = getInnerGeometry(inner);
				Geometry shared = m_intersection.apply(m_outGeom, inGeom);

				Geometries type = Geometries.get(shared);
				if ( type != Geometries.POLYGON && type != Geometries.MULTIPOLYGON  ) {
					getLogger().warn("found an incompatible output Geometry, type={}", type);
					shared = GeoUtils.cast(shared, MultiPolygon.class);
				}
				if ( !shared.isEmpty() ) {
					m_binding.put("right", inner);
					m_selector.select(m_binding, record);
					record.set(getOuterGeomColumn().ordinal(), shared);
					
					return true;
				}
				else {
					getLogger().debug("ignore empty output Geometry");
				}
			}

			return false;
		}
	}
}
