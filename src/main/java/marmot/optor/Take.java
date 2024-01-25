package marmot.optor;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.rset.SingleInputRecordSet;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.proto.optor.TakeProto;
import marmot.support.PBSerializable;


/**
 * {@code Take} 연산은 입력 레코드 세트에 포함된 레코드들 중에서 주어진 갯수의 처음 레코드들만으로
 * 구성된 레코드 세트를 생성하는 작업을 수행한다.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Take extends AbstractRecordSetFunction implements PBSerializable<TakeProto> {
	private final long m_count;
		
	public Take(long count) {
		m_count = count;
	}
	
	public long getCount() {
		return m_count;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		setInitialized(marmot, inputSchema, inputSchema);
	}

	@Override
	public RecordSet apply(RecordSet input) {
		checkInitialized();
		
		return new Taken(this, input);
	}

	@Override
	public MapReduceJoint getMapReduceJoint(RecordSchema inputSchema) {
		return MapReduceJoint.create()
					.setReducerCount(1)
					.addMapper(this)
					.addReducer(this);
	}
	
	@Override
	public String toString() {
		return String.format("take(%d)", m_count);
	}

	public static Take fromProto(TakeProto proto) {
		return new Take(proto.getCount());
	}

	@Override
	public TakeProto toProto() {
		return TakeProto.newBuilder()
						.setCount(m_count)
						.build();
	}
	
	private class Taken extends SingleInputRecordSet<Take> {
		private int m_takenCount = 0;

		Taken(Take func, RecordSet input) {
			super(func, input);
		}

		@Override
		public boolean next(Record record) {
			if ( m_takenCount+1 > m_count ) {
				return false;
			}
			else {
				++m_takenCount;
				return m_input.next(record);
			}
		}
		
		@Override
		public String toString() {
			return String.format("%s: remains=%d", m_optor, m_count - m_takenCount);
		}
	}
}