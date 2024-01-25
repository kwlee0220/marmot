package marmot.optor.geo.arc;

import java.util.List;

import org.geotools.geometry.jts.Geometries;
import org.locationtech.jts.geom.Geometry;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.geo.join.NestedLoopMatch;
import marmot.optor.geo.join.NestedLoopSpatialJoin;
import marmot.optor.support.SafeIntersection;
import marmot.optor.support.SafeUnion;
import marmot.plan.SpatialJoinOptions;
import marmot.proto.optor.ArcClipProto;
import marmot.support.PBSerializable;
import marmot.type.GeometryDataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ArcClip extends NestedLoopSpatialJoin<ArcClip> implements PBSerializable<ArcClipProto> {
	private final SafeIntersection m_intersection;
	private SafeUnion m_union;
	
	public ArcClip(String inputGeomCol, String paramDsId, SpatialJoinOptions opts) {
		super(inputGeomCol, paramDsId, opts);
		
		m_intersection = new SafeIntersection().setReduceFactor(0);
	}

	@Override
	protected RecordSchema initialize(MarmotCore marmot, RecordSchema outerSchema,
									RecordSchema innerSchema) {
		Column geomCol = getOuterGeomColumn();
		Geometries dstType = ((GeometryDataType)geomCol.type()).toGeometries();
		m_intersection.setResultType(dstType);
		m_union = new SafeUnion(dstType);
		
		return outerSchema;
	}

	@Override
	protected RecordSet doInnerLoop(NestedLoopMatch match) {
		Record result = match.getOuterRecord().duplicate();
		
		Geometry outerGeom = getOuterGeometry(match.getOuterRecord()); 	
		List<Geometry> clips = match.getInnerRecords()
									.map(inner -> getInnerGeometry(inner))
									.map(inner -> m_intersection.apply(outerGeom, inner))
									.toList();
		if ( clips.size() > 0 ) {
			result.set(getOuterGeomColumn().ordinal(), m_union.apply(clips));
			return RecordSet.of(result);
		}
		else {
			return RecordSet.empty(getRecordSchema());
		}
	}
	
	@Override
	public String toString() {
		return String.format("%s[%s<->%s]", getClass().getSimpleName(),
							getOuterGeomColumnName(), getParamDataSetId());
	}

	public static ArcClip fromProto(ArcClipProto proto) {
		SpatialJoinOptions opts = SpatialJoinOptions.fromProto(proto.getOptions());
		return new ArcClip(proto.getGeomColumn(), proto.getParamDataset(), opts);
	}

	@Override
	public ArcClipProto toProto() {
		return ArcClipProto.newBuilder()
									.setGeomColumn(getOuterGeomColumnName())
									.setParamDataset(getParamDataSetId())
									.setOptions(m_options.toProto())
									.build();
	}
}
