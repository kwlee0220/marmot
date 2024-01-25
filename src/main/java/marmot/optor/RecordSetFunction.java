package marmot.optor;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface RecordSetFunction extends RecordSetOperator {
	/**
	 * 레코드세트 함수를 초기화시킨다.
	 * 
	 * @param marmot	marmot 객체.
	 * @param inputSchema	입력 레코드세트의 스키마 정보.
	 */
	public void initialize(MarmotCore marmot, RecordSchema inputSchema);
	
	/**
	 * 레코드세트 함수 초기화시 설정된 입력 레코드세트 스키마를 반환한다.
	 * 
	 * @return	입력 레코드세트 스키마
	 */
	public RecordSchema getInputRecordSchema();
	
	/**
	 * Streamable 연산 여부를 반환한다.
	 * 
	 * @return	Streamable 여부.
	 */
	public default boolean isStreamable() {
		return true;
	}
	
	/**
	 * 반드시 MR 작업으로 처리되어야 하는지 여부를 반환한다.
	 * 
	 * @return	MR 작업 처리 여부
	 */
	public default boolean isMapReduceRequired() {
		return false;
	}
	
	/**
	 * 주어진 입력 레코드세트를 이용하여 본 연산을 수행한 결과 레코드세트를 반환한다.
	 * 
	 * @param input		입력 레코드세트.
	 * @return	연산 수행결과로 생성된 레코드세트 객체.
	 */
	public RecordSet apply(RecordSet input);
}