package marmot.optor.reducer;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.Take;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.proto.optor.TakeCombinerProto;
import marmot.proto.optor.TakeReducerProto;
import marmot.support.PBSerializable;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TakeReducer extends CombineableRecordSetReducer
						implements PBSerializable<TakeReducerProto> {
	private final long m_count;
	
	public static TakeReducer take(long count) {
		return new TakeReducer(count);
	}
	
	private TakeReducer(long count) {
		m_count = count;
	}
	
	public int getTakeCount() {
		return (int)m_count;
	}
	
	@Override
	public RecordSetReducer newIntermediateReducer() {
		return new TakeRecords(m_count);
	}

	public static TakeReducer fromProto(TakeReducerProto proto) {
		return TakeReducer.take(proto.getCount());
	}
	
	@Override
	public TakeReducerProto toProto() {
		return TakeReducerProto.newBuilder().setCount(m_count).build();
	}
	
	public static class TakeRecords extends AbstractRecordSetFunction
									implements RecordSetReducer,
											PBSerializable<TakeCombinerProto> {
		private final long m_count;
		
		protected TakeRecords(long count) {
			m_count = count;
		}

		@Override
		public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
			setInitialized(marmot, inputSchema, inputSchema);
		}

		@Override
		public RecordSet apply(RecordSet input) {
			Take take = new Take(m_count);
			take.initialize(m_marmot, input.getRecordSchema());
			return take.apply(input);
		}
		
		public static TakeRecords fromProto(TakeCombinerProto proto) {
			return new TakeRecords(proto.getCount());
		}

		@Override
		public TakeCombinerProto toProto() {
			return TakeCombinerProto.newBuilder().setCount(m_count).build();
		}
	}
}
