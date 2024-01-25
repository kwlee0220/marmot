package marmot.mapreduce;

import org.apache.hadoop.fs.Path;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface HdfsWriterTerminal extends MapReduceTerminal {
	/**
	 * MapReduce 작업의 최종 결과가 저장될 파일 경로명을 반환한다.
	 * 
	 * @return	결과파일 경로명.
	 */
	public Path getOutputPath();
}
