package marmot.optor.geo.join;

import java.util.Iterator;

import org.locationtech.jts.geom.Geometry;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.support.SafeDifference;
import marmot.plan.SpatialJoinOptions;
import marmot.proto.optor.SpatialDifferenceJoinProto;
import marmot.support.PBSerializable;
import marmot.type.DataType;
import marmot.type.GeometryDataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialDifferenceJoin extends NestedLoopSpatialJoin<SpatialDifferenceJoin>
									implements PBSerializable<SpatialDifferenceJoinProto> {
	private SafeDifference m_difference;
	
	public SpatialDifferenceJoin(String inputGeomCol, String paramDsId, SpatialJoinOptions opts) {
		super(inputGeomCol, paramDsId, opts);
	}

	@Override
	protected RecordSchema initialize(MarmotCore marmot, RecordSchema outerSchema,
									RecordSchema innerSchema) {
		Column outerGeomCol = getOuterGeomColumn();
		GeometryDataType geomType = (GeometryDataType)outerGeomCol.type();
		
		m_difference = new SafeDifference(geomType.toGeometries()).setReduceFactor(0);
		
		if ( geomType == DataType.POLYGON ) {
			return outerSchema.toBuilder()
						.addOrReplaceColumn(outerGeomCol.name(), DataType.MULTI_POLYGON)
						.build();
		}
		else if ( geomType == DataType.LINESTRING ) {
			return outerSchema.toBuilder()
					.addOrReplaceColumn(outerGeomCol.name(), DataType.MULTI_LINESTRING)
					.build();
		}
		
		return outerSchema;
	}

	@Override
	protected RecordSet doInnerLoop(NestedLoopMatch match) {
		// outer 레이어의 각 레코드에 대해 matching되는 모든 inner record들이
		// 존재하는 경우, outer 레코드의 공간 정보에서 매칭되는 모든 inner record들의
		// 공간 정보 부분을 모두 제거한다.
		//
		boolean untouched = true;
		
		Geometry outGeom = getOuterGeometry(match.getOuterRecord());
		Iterator<Record> iter = match.getInnerRecords().iterator();
		while ( iter.hasNext() && !outGeom.isEmpty() ) {
			Geometry inGeom = getInnerGeometry(iter.next());
			outGeom = m_difference.apply(outGeom, inGeom);
			
			untouched = false;
		}
		
		if ( !outGeom.isEmpty() ) {
			if ( untouched ) {
				++m_untoucheds;
			}
			else {
				++m_shrinkeds;
			}
			
			Record result = match.getOuterRecord().duplicate();
			result.set(getOuterGeomColumn().ordinal(), outGeom);
			return RecordSet.of(result);
		}
		else {
			++m_eraseds;
			return RecordSet.empty(getOuterRecordSchema());
		}
	}

	public static SpatialDifferenceJoin fromProto(SpatialDifferenceJoinProto proto) {
		SpatialJoinOptions opts = SpatialJoinOptions.fromProto(proto.getOptions());
		return new SpatialDifferenceJoin(proto.getGeomColumn(), proto.getParamDataset(), opts);
	}

	@Override
	public SpatialDifferenceJoinProto toProto() {
		return SpatialDifferenceJoinProto.newBuilder()
										.setGeomColumn(getOuterGeomColumnName())
										.setParamDataset(getParamDataSetId())
										.setOptions(m_options.toProto())
										.build();
	}

	private long m_untoucheds = 0;
	private long m_shrinkeds = 0;
	private long m_eraseds = 0;
	
	@Override
	public String toString() {
		return String.format("difference_join[{%s}<->%s]",
								getOuterGeomColumnName(), getParamDataSetId());
		
	}

	@Override
	protected String toString(long outerCount, long count, long velo, int clusterLoadCount) {
		String str = String.format("%s: untoucheds=%d, shrinkeds=%d, eraseds=%d",
									this, m_untoucheds, m_shrinkeds, m_eraseds);
		if ( velo >= 0 ) {
			str = str + String.format(", velo=%d/s", velo);
		}
		str = str + String.format(", load_count=%d", clusterLoadCount);
		
		return str;
	}
}
