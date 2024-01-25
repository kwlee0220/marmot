package marmot.optor;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import marmot.MarmotCore;
import marmot.io.HdfsPath;
import marmot.mapreduce.MapReduceJobConfigurer;
import marmot.mapreduce.PlanMRExecutionMode;
import utils.func.FOption;
import utils.func.UncheckedFunction;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface MapReduceableRecordSetLoader extends RecordSetLoader, MapReduceJobConfigurer {
	/**
	 * 사용하는 입력 파일 경로 이름들의 리스트를 반환한다.
	 * 본  메소드는 MapReduce 작업 수행용보다는 로그 메시지 출력시 활용된다.
	 * 
	 * @return	입력 파일 경로 이름들의 리스트
	 */
	public String getInputString();
	
	/**
	 * 본 RecordSetLoader의 지역 수행 여부를 반환한다.
	 * 입력 파일의 크기가 크지 않은 경우는 MapReduce를 활용하는 것보다 local하게 HDFS에 연결해서 수행하는 것이
	 * MapReduce의 기본 부하를 회피할 수 있어 수행 시간이 짧아질 수 있다.
	 * 
	 * @param marmot	Hadoop 설정 정보.
	 * @return	지역 수행 여부
	 * @throws IOException	수행 여부 판별시 오류가 발생된 경우.
	 */
	public PlanMRExecutionMode getExecutionMode(MarmotCore marmot) throws IOException;
	
	public default FOption<RecordSetFunction> getMapperRecordSetFunction() {
		return FOption.empty();
	}
	
	@SuppressWarnings("deprecation")
	public static long getBlockSize(FileSystem fs, List<Path> pathList) {
		return HdfsPath.walkRegularFileTree(fs, pathList)
						.map(UncheckedFunction.sneakyThrow(HdfsPath::getFileStatus))
						.mapToLong(FileStatus::getBlockSize)
						.findFirst()
						.getOrElse(fs.getDefaultBlockSize());
	}
}
