package marmot.optor;

import org.slf4j.LoggerFactory;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.rset.SingleInputRecordSet;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.proto.optor.DropProto;
import marmot.support.PBSerializable;


/**
 * Drop 연산자 클래스를 정의한다.
 * <p>
 * Drop 연산은 입력 레코드 세트에 포함된 레코드들 중에서 주어진 갯수의 처음 레코드들을
 * 제외한 레코드 세트를 생성하는 작업을 수행한다.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Drop extends AbstractRecordSetFunction
					implements NonParallelizable, PBSerializable<DropProto> {
	private final long m_count;

	/**
	 * Drop 연산자 객체를 생성한다.
	 * Drop 연산은 입력 레코드 세트에 포함된 레코드들 중에서 주어진 갯수의 처음 레코드들을
	 * 제외하는 작업을 수행한다.
	 * 
	 * @param count	제외시킬 처음 레코드의 갯수.
	 */
	public Drop(long count) {
		m_count = count;
		
		setLogger(LoggerFactory.getLogger(Drop.class));
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		setInitialized(marmot, inputSchema, inputSchema);
	}

	@Override
	public RecordSet apply(RecordSet input) {
		checkInitialized();
		
		return new Dropped(this, input);
	}
	
	@Override
	public String toString() {
		return "drop[count=" + m_count + "]";
	}

	public static Drop fromProto(DropProto proto) {
		return new Drop(proto.getCount());
	}

	@Override
	public DropProto toProto() {
		return DropProto.newBuilder()
						.setCount(m_count)
						.build();
	}
	
	private static class Dropped extends SingleInputRecordSet<Drop> {
		private long m_ndroppeds =0;
		
		Dropped(Drop drop, RecordSet input) {
			super(drop, input);
		}
		
		@Override
		public boolean next(Record record) {
			checkNotClosed();
			
			if ( m_ndroppeds < m_optor.m_count ) {
				while ( m_input.next(record) ) {
					if ( ++m_ndroppeds > m_optor.m_count ) {
						return true;
					}
				}
				
				return false;
			}
			
			return m_input.next(record);
		}
		
		@Override
		public String toString() {
			return String.format("%s: drop_count=%d", m_optor, m_ndroppeds);
		}
	}
}