package marmot.optor.geo.join;

import java.util.Iterator;
import java.util.Map;

import com.google.common.collect.Maps;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSets;
import marmot.optor.RecordSetOperatorException;
import marmot.optor.support.JoinUtils;
import marmot.optor.support.colexpr.ColumnSelector;
import marmot.plan.SpatialJoinOptions;
import marmot.proto.optor.SpatialOuterJoinProto;
import marmot.support.PBSerializable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialOuterJoin extends NestedLoopSpatialJoin<SpatialOuterJoin>
							implements PBSerializable<SpatialOuterJoinProto> {
	private ColumnSelector m_selector;
	
	public SpatialOuterJoin(String inputGeomCol, String paramDsId, SpatialJoinOptions opts) {
		super(inputGeomCol, paramDsId, opts);
	}

	@Override
	protected RecordSchema initialize(MarmotCore marmot, RecordSchema outerSchema,
										RecordSchema innerSchema) {
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
		Iterator<Record> innerIter = match.getInnerRecords().iterator();
		if ( !innerIter.hasNext() ) {
			return RecordSets.singleton(getRecordSchema(), record -> {
				Map<String,Record> binding = Maps.newHashMap();
				binding.put("left", match.getOuterRecord());
				m_selector.select(binding, record);
			});
		}
		else {
			return m_selector.select(match.getOuterRecord(),
									RecordSet.from(match.getInnerRecordSchema(), innerIter));
		}
	}

	public static SpatialOuterJoin fromProto(SpatialOuterJoinProto proto) {
		SpatialJoinOptions opts = SpatialJoinOptions.fromProto(proto.getOptions());
		return new SpatialOuterJoin(proto.getGeomColumn(), proto.getParamDataset(), opts);
	}

	@Override
	public SpatialOuterJoinProto toProto() {
		return SpatialOuterJoinProto.newBuilder()
									.setGeomColumn(getOuterGeomColumnName())
									.setParamDataset(getParamDataSetId())
									.setOptions(m_options.toProto())
									.build();
	}
}
