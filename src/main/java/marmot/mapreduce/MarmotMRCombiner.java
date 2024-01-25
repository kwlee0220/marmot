package marmot.mapreduce;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.MarmotCore;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.exec.MarmotExecutionException;
import marmot.io.RecordWritable;
import marmot.mapreduce.support.MarmotReducerContext;
import marmot.support.RecordSetOperatorChain;
import utils.Throwables;
import utils.UnitUtils;
import utils.Utilities;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotMRCombiner extends Reducer<Writable, RecordWritable, Writable, Writable>
								implements MarmotReducerContext {
	private static final Logger s_logger = LoggerFactory.getLogger(MarmotMRCombiner.class);
	
	protected Context m_context;
	private MarmotCore m_marmot;

	private Plan m_plan;
	private RecordSchema m_inputSchema;
	private RecordSchema m_outputSchema;
	
	private int m_keyReadCount = 0;
	private long m_startedMillis;

	@SuppressWarnings("rawtypes")
	@Override
	public TaskInputOutputContext getHadoopContext() {
		return m_context;
	}

	@Override
	public MarmotCore getMarmotCore() {
		Utilities.checkState(m_marmot != null, "MarmotCore has not been set");
		
		return m_marmot;
	}

	@Override
	public RecordSchema getInputRecordSchema() {
		return m_inputSchema;
	}

	@Override
	public RecordSchema getOutputRecordSchema() {
		return m_outputSchema;
	}

	@Override
	public Plan getPlan() {
		return m_plan;
	}

	@Override
	public int getNumTasks() {
		String ntasksStr = m_context.getConfiguration().get("mapred.map.tasks");
		return Integer.parseInt(ntasksStr);
	}

	@Override
	public long getKeyInputCount() {
		return m_keyReadCount;
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

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		m_context = context;
		m_startedMillis = System.currentTimeMillis();
		
		m_marmot = new MarmotCore(context);
		Configuration conf = m_marmot.getHadoopConfiguration();

		MarmotMRContexts.set(this);
		try {	
			configureMarmotLoggers();
			
			m_plan = MapReduceStage.getCombinerPlan(conf);
			m_inputSchema = MapReduceStage.getCombinerInputRecordSchema(conf);
			RecordSetOperatorChain chain = RecordSetOperatorChain.from(m_marmot, m_plan,
																		m_inputSchema);
			
			FOption<MarmotMapOutputKeyColumns> mokCols = MapReduceStage.getMapOutputKeyColumns(conf);
			chain.setRecordSetConsumer(new MapOutputWriter(mokCols));
			chain.streamOperators()
					.castSafely(MarmotMRContextAware.class)
					.forEach(optor -> optor.setMarmotMRContext(this));
			
			if ( s_logger.isInfoEnabled() ) {
				s_logger.info("plan for combiner: " + m_plan.getName());
				chain.traceLog(s_logger);
			}
			m_outputSchema = chain.getOutputRecordSchema();

			RecordSet input = new ReduceContextRecordSet(context, m_inputSchema);
			s_logger.info("initialized: {}", this);

			MarmotMRContexts.runWithPeriodicReport(this, chain, input, m_startedMillis);
			
			long elapsed = System.currentTimeMillis() - m_startedMillis;
			s_logger.info("finished: reducer={} elapsed={}",
								this, UnitUtils.toMillisString(elapsed));
		}
		catch ( Throwable e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			s_logger.error("fails to load reducer plan", cause);
			throw new MarmotExecutionException("fails to load reducer plan", cause);
		}
		finally {
			MarmotMRContexts.unset();
		}
	}
	
	@Override
	public String toString() {
		return String.format("%s[%d]", getClass().getSimpleName(), getTaskOrdinal());
	}
	
	private void configureMarmotLoggers() {
		FileSystem fs = m_marmot.getHadoopFileSystem();
		
		Path propsPath = new Path("log4j_marmot.properties");
		s_logger.info("loading log4j.properties from {}", propsPath);
		
		try ( InputStream is = fs.open(propsPath) ) {
			Properties props = new Properties();
			props.load(is);
			
			props.keySet().stream()
				.map(o -> (String)o)
				.filter(key -> key.startsWith("log4j.logger."))
				.forEach(key -> {
					String name = key.substring(13);
//					Logger logger = LoggerFactory.getLogger(name);
//					String levelStr = props.getProperty(key);
//					Level level = Level.valueOf(levelStr);
//					
//					logger.
				});
		}
		catch ( Exception e ) {
			s_logger.warn("fails to load Marmot log4j.properties: " + propsPath);
		}
	}
}
