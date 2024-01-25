package marmot.module;

import java.util.List;
import java.util.Map;

import marmot.MarmotCore;
import marmot.RecordSchema;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface MarmotModule extends Runnable {
	/**
	 * 본 모듈 수행에 필요한 인자 이름 리스트를 반환한다.
	 * 
	 * @return	 인자 이름 리스트.
	 */
	public List<String> getParameterNameAll();
	
	/**
	 * 주어진 인자에 대한 출력 레코드 스키마를 반환한다.
	 * 
	 * @param marmot	{@link MarmotCore} 객체
	 * @param params	인자 목록
	 * @return	 결과 레코드 스키마.
	 */
	public RecordSchema getOutputRecordSchema(MarmotCore marmot, Map<String, String> params);
	
	/**
	 * 주어진 인자를 활용하여 모듈을 초기화시킨다. 
	 * 
	 * @param marmot	{@link MarmotCore} 객체
	 * @param params	인자 목록
	 */
	public void initialize(MarmotCore marmot, Map<String,String> params);
}
