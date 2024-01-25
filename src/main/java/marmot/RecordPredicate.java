package marmot;

import java.util.function.Predicate;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface RecordPredicate extends Predicate<Record> {
	/**
	 * RecordPredicate의 입력으로 사용될 레코드의 스키마를 설정한다.
	 * 
	 * @param marmot	Marmot 객체
	 * @param inputSchema	입력 레코드 스키마.
	 */
	public void initialize(MarmotCore marmot, RecordSchema inputSchema);
}
