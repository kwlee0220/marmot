package marmot.optor.geo.join;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import marmot.MarmotCore;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.mapreduce.MarmotMRContext;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class FileInputFormatMRContext implements MarmotMRContext {
	private final TaskAttemptContext m_context;
	private final MarmotCore m_marmot;
	private final long m_startedMillis;
	
	FileInputFormatMRContext(TaskAttemptContext context) {
		m_context = context;
		m_marmot = new MarmotCore(context);
		
		m_startedMillis = System.currentTimeMillis();
	}

	@Override
	public MarmotCore getMarmotCore() {
		return m_marmot;
	}

	@Override
	public int getNumTasks() {
		String ntasksStr = m_context.getConfiguration().get("mapred.map.tasks");
		return Integer.parseInt(ntasksStr);
	}

	@Override
	public int getTaskOrdinal() {
		return m_context.getTaskAttemptID().getTaskID().getId();
	}

	@Override
	public long getStartedMillis() {
		return m_startedMillis;
	}

	@Override
	public void reportProgress() {
		m_context.progress();
	}
	
	//
	// 이 클래스의 구체적인 구현 목적을 잊어벼려 이후 메소드는 구현하지 못했음.
	//

	@SuppressWarnings("rawtypes")
	@Override
	public TaskInputOutputContext getHadoopContext() {
		throw new AssertionError("should not be called!!!");
	}

	@Override
	public Plan getPlan() {
		throw new AssertionError("should not be called!!!");
	}

	@Override
	public RecordSchema getInputRecordSchema() {
		throw new AssertionError("should not be called!!!");
	}

	@Override
	public RecordSchema getOutputRecordSchema() {
		throw new AssertionError("should not be called!!!");
	}
}