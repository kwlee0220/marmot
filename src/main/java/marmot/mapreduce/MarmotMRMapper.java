package marmot.mapreduce;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.MarmotCore;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.exec.MarmotExecutionException;
import marmot.mapreduce.support.MarmotMapperContext;
import marmot.support.RecordSetOperatorChain;
import utils.Throwables;
import utils.UnitUtils;
import utils.Utilities;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotMRMapper extends Mapper<Writable, Writable, Writable, Writable>
							implements MarmotMapperContext {
	private static final Logger s_logger = LoggerFactory.getLogger(MarmotMRMapper.class);
	
	private Context m_context;
	private MarmotCore m_marmot;
	
	private long m_startedMillis;
	
	private RecordSchema m_inputSchema;
	private RecordSchema m_outputSchema;
	private Plan m_plan;

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
	
	public Plan getPlan() {
		return m_plan;
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
//					org.apache.log4j.Logger logger = LogManager.getLogger(name);
//					String levelStr = props.getProperty(key);
//					Level level = Level.toLevel(levelStr);
//					
//					logger.setLevel(level);
				});
		}
		catch ( Exception e ) {
			s_logger.warn("fails to load Marmot log4j.properties: " + propsPath, e);
		}
	}
	
	@Override
	public void run(final Context context) throws IOException ,InterruptedException {
		m_context = context;
		m_startedMillis = System.currentTimeMillis();
		
		m_marmot = new MarmotCore(context);
		Configuration conf = m_marmot.getHadoopConfiguration();
		
		configureMarmotLoggers();
		
		if ( s_logger.isInfoEnabled() ) {
			InputSplit split = context.getInputSplit();
			if ( split instanceof FileSplit ) {
				FileSplit fsplit = (FileSplit)split;
				s_logger.info(String.format("loading file: path=%s start=%s length=%s",
											fsplit.getPath(),
											UnitUtils.toByteSizeString(fsplit.getStart()),
											UnitUtils.toByteSizeString(fsplit.getLength())));
			}
			else {
				s_logger.info("loading split: {}", split);
			}
		}

		MarmotMRContexts.set(this);
		try {
			RecordSchema inputSchema = getInputRecordSchema(conf);
			m_plan = MapReduceStage.getMapperPlan(conf);
			RecordSetOperatorChain chain = RecordSetOperatorChain.from(m_marmot, m_plan, inputSchema);
			
			if ( s_logger.isInfoEnabled() ) {
				s_logger.info("plan for mapper: " + m_plan.getName());
				chain.traceLog(s_logger);
			}
			
			if ( chain.getRecordSetConsumer() == null ) {
				FOption<MarmotMapOutputKeyColumns> keyCols = MapReduceStage.getMapOutputKeyColumns(conf);
				chain.setRecordSetConsumer(new MapOutputWriter(keyCols));
			}
			chain.streamOperators()
					.castSafely(MarmotMRContextAware.class)
					.forEach(optor -> optor.setMarmotMRContext(this));

			m_inputSchema = chain.getInputRecordSchema();
			m_outputSchema = chain.getOutputRecordSchema();
			
			RecordSet input = new MapContextRecordSet(m_inputSchema, m_context);
			chain.setInputRecordSchema(m_inputSchema);
			
			s_logger.debug("initialized: mapper={}", this);
			
			MarmotMRContexts.runWithPeriodicReport(this, chain, input, m_startedMillis);
			
			long elapsed = System.currentTimeMillis() - m_startedMillis;
			s_logger.info("finished: mapper={} elapsed={}",
							this, UnitUtils.toMillisString(elapsed));
		}
		catch ( Throwable e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			s_logger.error("fails to load mapper plan", cause);
			throw new MarmotExecutionException("fails to load mapper plan", cause);
		}
		finally {
			MarmotMRContexts.unset();
		}
	}
	
	@Override
	public String toString() {
		return String.format("%s[%d]", getClass().getSimpleName(), getTaskOrdinal());
	}
	
	protected RecordSchema getInputRecordSchema(Configuration conf) {
		return MapReduceStage.getMapperInputSchema(conf);
	}
}