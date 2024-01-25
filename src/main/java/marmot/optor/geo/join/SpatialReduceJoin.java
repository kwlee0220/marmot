package marmot.optor.geo.join;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.RecordSetFunction;
import marmot.optor.reducer.ValueAggregateReducer;
import marmot.plan.SpatialJoinOptions;
import marmot.proto.optor.SpatialReduceJoinProto;
import marmot.rset.AbstractRecordSet;
import marmot.support.DefaultRecord;
import marmot.support.PBSerializable;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialReduceJoin extends NestedLoopSpatialJoin<SpatialReduceJoin>
									implements PBSerializable<SpatialReduceJoinProto> {
	private final RecordSetFunction m_reducer;
	
	public SpatialReduceJoin(String inputGeomCol, String paramDsId, RecordSetFunction reducer,
							SpatialJoinOptions opts) {
		super(inputGeomCol, paramDsId, opts);
		Utilities.checkNotNullArgument(reducer, "reducer is null");
		
		m_reducer = reducer;
	}

	@Override
	protected RecordSchema initialize(MarmotCore marmot, RecordSchema inputSchema,
										RecordSchema paramSchema) {
		m_reducer.initialize(marmot, paramSchema);
		RecordSchema reducedSchema = m_reducer.getRecordSchema();
		return  RecordSchema.concat(inputSchema, reducedSchema);
	}

	@Override
	protected RecordSet doInnerLoop(NestedLoopMatch match) {
		RecordSet reduceds = m_reducer.apply(match.getInnerRecordSet());
		return new Concatenateds(getRecordSchema(), match.getOuterRecord(), reduceds);
	}

	public static SpatialReduceJoin fromProto(SpatialReduceJoinProto proto) {
		ValueAggregateReducer reducer = ValueAggregateReducer.fromProto(proto.getReducer());
		SpatialJoinOptions opts = SpatialJoinOptions.fromProto(proto.getOptions());
		return new SpatialReduceJoin(proto.getGeomColumn(), proto.getParamDataset(),
										reducer, opts);
	}

	@Override
	public SpatialReduceJoinProto toProto() {
		ValueAggregateReducer reducer = (ValueAggregateReducer)m_reducer;
		return SpatialReduceJoinProto.newBuilder()
										.setGeomColumn(getOuterGeomColumnName())
										.setParamDataset(getParamDataSetId())
										.setReducer(reducer.toProto())
										.setOptions(m_options.toProto())
										.build();
	}
	
	private static class Concatenateds extends AbstractRecordSet {
		private final RecordSchema m_schema;
		private final Record m_keyRecord;
		private final RecordSet m_reduceds;
		private final int m_keyLength;
		private final Record m_reduced;
		
		private Concatenateds(RecordSchema schema, Record keyRecord, RecordSet reduceds) {
			m_schema = schema;
			m_keyRecord = keyRecord;
			m_reduceds = reduceds;
			
			m_keyLength = keyRecord.getColumnCount();
			m_reduced = DefaultRecord.of(m_reduceds.getRecordSchema());
		}

		@Override
		protected void closeInGuard() throws Exception {
			m_reduceds.close();
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_schema;
		}
		
		@Override
		public boolean next(Record output) {
			if ( !m_reduceds.next(m_reduced) ) {
				return false;
			}
			
			output.set(m_keyRecord);
			output.setAll(m_keyLength, m_reduced.getAll());
			
			return true;
		}
	}
}
