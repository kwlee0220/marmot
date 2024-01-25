package marmot.mapreduce;

import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import marmot.MarmotCore;
import marmot.MarmotInternalException;
import marmot.RecordSet;
import marmot.exec.MarmotExecutionException;
import marmot.optor.RecordSetConsumer;
import marmot.support.PeriodicProgressReporter;
import marmot.support.ProgressReportable;
import marmot.support.RecordSetOperatorChain;
import utils.StopWatch;
import utils.UnitUtils;
import utils.func.FOption;
import utils.thread.RecurringScheduleThread;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotMRContexts {
	private static final Logger s_logger = LoggerFactory.getLogger(MarmotMRContexts.class);
	
	private static final long PROGRESS_REPORT_INTERVAL = TimeUnit.MINUTES.toMillis(3);
	private static ThreadLocal<MarmotMRContext> s_context = new ThreadLocal<>();
	private static ThreadLocal<Long> s_lastReported = new ThreadLocal<>();
	
	public static FOption<MarmotMRContext> get() {
		return FOption.ofNullable(s_context.get());
	}
	
	public static MarmotMRContext getOrNull() {
		return s_context.get();
	}
	
	public static void set(MarmotMRContext context) {
		Preconditions.checkArgument(context != null, "MarmotMRContext is null");
		
		s_context.set(context);
		s_lastReported.set(context.getStartedMillis());
		
		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug("set {}: thread={}", MarmotMRContexts.class.getSimpleName(),
												Thread.currentThread().getName());
		}
	}
	
	public static void unset() {
		s_context.remove();
		s_lastReported.remove();

		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug("unset {}: thread={}", MarmotMRContexts.class.getSimpleName(),
												Thread.currentThread().getName());
		}
	}
	
	public static boolean isMapReduceMode() {
		return get().isPresent();
	}
	
	/**
	 * 본 쓰레드를 통해 수행 중인 mapper또는 reducer의 task 번호를 반환한다.
	 * 만일 맵리듀스 작업이 수행 중이지 않은 경우는 {@link FOption#empty()}가 반환된다.
	 * 
	 * @return	Task 번호.
	 */
	public static FOption<Integer> getTaskOrdinal() {
		MarmotMRContext context = s_context.get();
		return context != null ? FOption.of(context.getTaskOrdinal()) : FOption.empty();
	}
	
	public static void reportProgress() {
		long now = System.currentTimeMillis();
		Long last = s_lastReported.get();
		if ( last != null ) {
			long elapsed = now - last;
			if ( elapsed >= PROGRESS_REPORT_INTERVAL ) {
				MarmotMRContext ctxt = s_context.get();
				if ( ctxt != null ) {
					ctxt.reportProgress();
					s_lastReported.set(now);

					if ( s_logger.isInfoEnabled() ) {
						s_logger.info("report progress: elapsed={}",
									UnitUtils.toMillisString(now - ctxt.getStartedMillis()));
					}
				}
			}
		}
	}
	
	static void configureMarmotMRLogger(MarmotCore marmot) {
		FileSystem fs = marmot.getHadoopFileSystem();
		
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
			s_logger.warn("fails to load Marmot log4j.properties: " + propsPath);
		}
	}
	
	private static final class MRReporter implements ProgressReportable {
		private final MarmotMRContext m_context;
		private final ProgressReportable m_consumer;
		
		MRReporter(MarmotMRContext context, RecordSetConsumer consumer) {
			m_context = context;
			m_consumer = (consumer instanceof ProgressReportable)
						? (ProgressReportable) consumer: null;
		}
		
		@Override
		public void reportProgress(Logger logger, StopWatch elapsed) {
			m_context.reportProgress();
			if ( m_consumer != null ) {
				m_consumer.reportProgress(logger, elapsed);
			}
		}
	}
	static void runWithPeriodicReport(MarmotMRContext context, RecordSetOperatorChain chain,
										RecordSet input, long startedMillis) {
		RecordSetConsumer consumer = chain.getRecordSetConsumer();
		if ( consumer == null ) {
			throw new MarmotExecutionException("invalid plan (no RecordSetConsumer): " + chain);
		}
		
		// '진행사항 보고 시간'이 설정되어 있지 않은 경우는 그냥 plan만 수행시킨다.
		long reportInterval = context.getMarmotCore()
									.getProgressReportIntervalSeconds()
									.map(TimeUnit.SECONDS::toMillis)
									.getOrElse(-1L);
		if ( reportInterval< 0 ) {
			chain.run(input);
			return;
		}
		
		MRReporter reporter = new MRReporter(context, consumer);
		RecurringScheduleThread reporterThread = null;
		try {
			reporterThread = PeriodicProgressReporter.create(ProgressReportable.s_logger, reporter,
																reportInterval);
			try {
				reporterThread.start();
			}
			catch ( Exception e ) {
				throw new MarmotInternalException("" + e);
			}
			chain.run(input);

			reporter.reportProgress(s_logger, StopWatch.start(startedMillis));
		}
		finally {
			if ( reporter != null ) {
				reporterThread.stop(true);
			}
		}
	}
}
