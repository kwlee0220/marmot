package marmot.optor.geo.join;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.RecordSetOperatorException;
import marmot.optor.support.JoinUtils;
import marmot.optor.support.colexpr.ColumnSelector;
import marmot.plan.SpatialJoinOptions;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class NestedLoopSpatialInnerJoin extends NestedLoopSpatialJoin<NestedLoopSpatialInnerJoin> {
	private ColumnSelector m_selector;
	
	public NestedLoopSpatialInnerJoin(String inputGeomCol, String paramDataSet,
										SpatialJoinOptions opts) {
		super(inputGeomCol, paramDataSet, opts);
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
		RecordSet inners = RecordSet.from(match.getInnerRecordSchema(), match.getInnerRecords());
		return m_selector.select(match.getOuterRecord(), inners);
	}
}
