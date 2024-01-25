package marmot.optor;

import marmot.MarmotCore;
import marmot.RecordSet;


/**
 * {@code RecordSetLoader}는 Marmot 시스템 외부에 위치한 데이터를로부터 레코드들을 읽어
 * 레코드 세트를 생성하는 레코드 세트 적재기 인터페이스를 정의한다.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface RecordSetLoader extends RecordSetOperator {
	/**
	 * 레코드세트 적재기를 초기화시킨다.
	 * 
	 * @param marmot	초기화시 사용할 {@link MarmotCore} 객체.
	 */
	public void initialize(MarmotCore marmot);
	
	/**
	 * 레코드 세트를 적재한다.
	 * 
	 * @return	본 레코드 세트 적재기를 통해 적재된 레코드 세트.
	 */
	public RecordSet load();
}
