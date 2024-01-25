package marmot.optor;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface RecordSetConsumer extends RecordSetOperator {
	/**
	 * 레코드세트 함수를 초기화시킨다.
	 * 
	 * @param marmot		초기화시 사용할 {@link MarmotCore} 객체
	 * @param inputSchema	입력 레코드세트의 스키마 정보.
	 */
	public void initialize(MarmotCore marmot, RecordSchema inputSchema);
	
	/**
	 * 소모시킬 레코드 세트의 스키마를 반환한다.
	 * 
	 * @return	레코드세트 스키마 객체
	 */
	public RecordSchema getInputRecordSchema();
	
	/**
	 * 주어진 레코드 세트에 포함된 모든 레코드를 수집한다. 
	 * 
	 * @param rset	수집할 레코드 세트.
	 */
	public void consume(RecordSet rset);
	
	public default RecordSchema getOutputRecordSchema() {
		return RecordSchema.NULL;
	}
}
