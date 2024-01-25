package marmot.mapreduce;

import org.apache.hadoop.fs.Path;

import marmot.RecordSchema;
import marmot.io.HdfsPath;
import utils.async.AbstractThreadedExecution;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class Stage extends AbstractThreadedExecution<Void> {
	public abstract String getId();
	
	/**
	 * Stage 수행 결과 HDFS에 결과 파일이 생성될 경우 해당 파일의 경로명을 반환한다.
	 * <p>
	 * HDFS에 별도의 파일이 생성되지 않는 경우 (예를들어 결과가 JDBC를 통해 DBMS에
	 * 저장되는 경우, 혹은 JMS를 통해 Queue에 저장되는 경우)는 {@link FOption#empty}이
	 * 반환된다.
	 * 
	 * @return	HDFS내 결과 파일의 경로명.
	 */
	public abstract FOption<HdfsPath> getOutputPath();
	
	/**
	 * Stage 수행 중에 사용할 수 있는 HDFS 작업공간 영역의 최상위 경로명을 반환한다.
	 * 
	 * @return	작업공간 경로명.
	 */
	public abstract Path getWorkspace();
	
	/**
	 * Stage 수행 결과의 레코드세트의 스키마를 반환한다.
	 * 
	 * @return	수행 결과 레코드세트 스키마.
	 */
	public abstract RecordSchema getOutputRecordSchema();
}
