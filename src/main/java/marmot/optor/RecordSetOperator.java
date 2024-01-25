package marmot.optor;

import marmot.MarmotCore;
import marmot.RecordSchema;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface RecordSetOperator {
	/**
	 * MarmotRuntime 객체를 반환한다.
	 * 
	 * @return	설정된 MarmotRuntime 객체.
	 */
	public MarmotCore getMarmotCore();
	
	/**
	 * 레코드 세트 연산자의 초기화 여부를 반환한다.
	 * 
	 * @return	초기화 여부
	 */
	public boolean isInitialized();
	
	public default void checkInitialized() {
		if ( !isInitialized() ) {
			throw new IllegalArgumentException("not initialized: optor=" + this);
		}
	}
	
	/**
	 * 본 연산자를 통해 생성될 레코드 스트림의 스키마를 반환한다.
	 * 
	 * @return	레코드 스트림 스키마.
	 */
	public RecordSchema getRecordSchema();
	
	/**
	 * MapReduceJoint 정보를 반환한다.
	 * MapReduceJoint가 아닌 연산인 경우는 {@code null}을 반환한다.
	 * 
	 * @param inputSchema	입력 레코드세트의 스키마
	 * @return	MapReduceJoint 객체.
	 */
	public default MapReduceJoint getMapReduceJoint(RecordSchema inputSchema) {
		return null;
	}
}