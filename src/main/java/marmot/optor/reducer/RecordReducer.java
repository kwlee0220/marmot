package marmot.optor.reducer;

import marmot.Record;
import marmot.RecordSet;
import marmot.optor.RecordSetFunction;
import marmot.support.DefaultRecord;


/**
 * {@link RecordReducer}는 레코드 세트를 하나의 레코드와 reduce 시키는 연산자의
 * 인터페이스를 정의한다.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface RecordReducer extends RecordSetFunction {
	public void reduce(Record accum, Record input);
	
	/**
	 * 주어진 레코드 세트를 단일 레코드로 변환하는 reduce 작업을 수행한다.
	 * 
	 * @param rset	reduce 대상이 되는 입력 레코드세트.
	 * @param reduced reduce된 결과 레코드.
	 */
	public default void reduce(RecordSet rset, Record reduced) {
		Record input = DefaultRecord.of(rset.getRecordSchema());
		while ( rset.next(input) ) {
			reduce(reduced, input);
		}
	}

	/**
	 * 주어진 레코드세트를 reduce한 결과를 얻는다.
	 * 
	 * @param rset		reduce할 대상 레코드세트
	 * @return	reduce된 결과 레코드
	 */
	public default Record reduce(RecordSet rset) {
		Record reduced = DefaultRecord.of(getRecordSchema());
		reduce(rset, reduced);
		return reduced;
	}

	@Override
	public default RecordSet apply(RecordSet input) {
		checkInitialized();
		
		return RecordSet.lazy(getRecordSchema(), () -> RecordSet.of(reduce(input)));
	}
}
