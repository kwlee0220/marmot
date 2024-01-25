package marmot;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface RecordTransform {
	/**
	 * RecordTransform의 입력으로 사용될 레코드의 스키마를 설정한다.
	 * <p>
	 * 본 메소드는 {@link #getRecordSchema()}와 {@link #transform(Record, Record)} 메소드
	 * 호출 이전에 호출되어야 한다.
	 * 만일 그렇지 않은 경우 위 두 메소드가 호출되는 경우의 동작은 미정의된다.
	 * 
	 * @param marmot	Marmot 객체
	 * @param inputSchema	입력 레코드 스키마.
	 */
	public void initialize(MarmotCore marmot, RecordSchema inputSchema);
	
	/**
	 * 본 RecordTransform에 의해 변형된 레코드의 스키마를 반환한다.
	 * <p>
	 * 본 메소드 호출 이전에 반드시
	 * {@link #initialize(MarmotCore,RecordSchema)}가 호출되어야 한다.
	 * 
	 * @return	변형된 레코드의 스키마.
	 */
	public RecordSchema getRecordSchema();
	
	/**
	 * 주어진 입력 레코드을 변형하여 그 결과를 출력 레코드에 저장한다.
	 * 
	 * @param input		입력 레코드
	 * @param output	결과가 저장될 출력 레코드
	 * @return	변환 성공 여부..
	 */
	public boolean transform(Record input, Record output);
}
