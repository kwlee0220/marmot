package marmot.optor.geo.join;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.plan.SpatialJoinOptions;
import marmot.proto.optor.SpatialSemiJoinProto;
import marmot.support.PBSerializable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialSemiJoin extends NestedLoopSpatialJoin<SpatialSemiJoin>
							implements PBSerializable<SpatialSemiJoinProto> {
	private boolean m_negated = false;
	
	public SpatialSemiJoin(String inputGeomCol, String paramDsId, SpatialJoinOptions opts) {
		super(inputGeomCol, paramDsId, opts);
		
		m_negated = opts.negated().getOrElse(false);
	}

	@Override
	protected RecordSchema initialize(MarmotCore marmot, RecordSchema outerSchema,
										RecordSchema innerSchema) {
		return outerSchema;
	}

	@Override
	protected RecordSet doInnerLoop(NestedLoopMatch match) {
		boolean isEmpty = !match.getInnerRecords().next().isPresent();
		boolean hasMatches = m_negated ? isEmpty : !isEmpty;
		if ( hasMatches ) {
			return RecordSet.of(match.getOuterRecord());
		}
		else {
			return RecordSet.empty(getOuterRecordSchema());
		}
	}
	
	@Override
	public String toString() {
		String negateStr = (m_negated) ? ", negated" : "";
		return String.format("%s[param=%s, join=%s%s]", getClass().getSimpleName(),
								getParamDataSetId(), getJoinExpr(), negateStr);
	}

	@Override
	protected String toString(long outerCount, long count, long velo, int clusterLoadCount) {
		String str = String.format("%s: %d->%d(%.1f%%)",
									this, outerCount, count, ((double)count/outerCount)*100);
		if ( velo >= 0 ) {
			str = str + String.format(", velo=%d/s", velo);
		}
		str = str + String.format(", cluster_load_count=%d", clusterLoadCount);
		
		return str;
	}

	public static SpatialSemiJoin fromProto(SpatialSemiJoinProto proto) {
		SpatialJoinOptions opts = SpatialJoinOptions.fromProto(proto.getOptions());
		return new SpatialSemiJoin(proto.getGeomColumn(), proto.getParamDataset(), opts)
					.setSemiJoin(true);
	}

	@Override
	public SpatialSemiJoinProto toProto() {
		return SpatialSemiJoinProto.newBuilder()
										.setGeomColumn(getOuterGeomColumnName())
										.setParamDataset(getParamDataSetId())
										.setOptions(m_options.toProto())
										.build();
	}
}
