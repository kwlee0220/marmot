package marmot.mapreduce.support;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;

import marmot.Plan;
import marmot.Record;


/**
 * MapReduce job에서 마지막 task의 인터페이스를 정의한다.
 * MapReduce 방식으로 처리될 {@link Plan}의 마지막 연산자는
 * 반드시 {@code MapReduceContextWriter} 인터페이스를 구현하여야 한다.
 * 
 * 본 인터페이스를 통해 MapReduce의 {@link Job} 객체를 초기화할 때 사용한다.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface MapReduceContextWriter<T extends Writable> {
	/**
	 * 생성될 파일의 형식을 반환한다.
	 * 
	 * MapReduce 과정에서 작업의 OutputFormatClass를 설정할 때
	 * ((@link Job#setOutputFormatClass}) 활용된다.
	 * 
	 * @return	출력 파일 형식
	 */
	@SuppressWarnings("rawtypes")
	public Class<? extends OutputFormat> getOutputFormatClass();
	
	/**
	 * 출력 데이터를 위한 {{@link Writable} 클래스를 반환한다.
	 * 
	 * Map only 작업인 경우, 작업의 MapOutputValueClass를 설정하기 위해
	 * {@link Job#setMapOutputValueClass} 메소드를 호출할 때 활용된댜.
	 * 
	 * @return	Writable class
	 */
	public Class<T> getOutputValueClass();
	
	/**
	 * 레코드를 출력하기 위한 {{@link Writable} 객체를 생성한다.
	 * 
	 * 레코드({@link Record})를 파일에 쓰기 위해 {@link Writable} 객체로
	 * 변환할 때 사용된다.
	 * <ul>
	 * 	<li> Map 작업 결과 레코드를 저장할 때
	 * 	<li> Combine 작업 결과 레코드를 저장할 때
	 * 	<li> Reduce 작업 결과 레코드를 저장할 때
	 * </ul>
	 * 
	 * @param record	출력 대상 레코드 객체.
	 * @return	Writable 객체.
	 */
	public T toValueWritable(Record record);
}
