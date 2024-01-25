package marmot.mapreduce;

import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import marmot.MarmotCore;
import marmot.Plan;
import marmot.RecordSchema;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface MarmotMRContext {
	public MarmotCore getMarmotCore();
	
	@SuppressWarnings("rawtypes")
	public TaskInputOutputContext getHadoopContext();

	public Plan getPlan();
	public RecordSchema getInputRecordSchema();
	public RecordSchema getOutputRecordSchema();
	
	public int getNumTasks();
	
	/**
	 * 맵리듀스 작업을 수행하는 task의 번호를 반환한다.
	 * Task 번호는 맵리듀스 수행 중 mapper 또는 reducer에게 부여된 번호를 의미하며,
	 * 0번부터 시작하여 해당 맵리듀스 작업 수행으로 생성된 mapper/reducer의 갯수만큼 부여된다. 
	 * 
	 * @return	Task 번호.
	 */
	public int getTaskOrdinal();
	public long getStartedMillis();
	public void reportProgress();
}
