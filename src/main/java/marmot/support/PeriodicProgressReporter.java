package marmot.support;

import org.slf4j.Logger;

import utils.StopWatch;
import utils.thread.RecurringSchedule;
import utils.thread.RecurringScheduleThread;
import utils.thread.RecurringWork;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PeriodicProgressReporter implements RecurringWork {
	private final Logger m_reportLogger;
	private final ProgressReportable m_reportable;
	private StopWatch m_elapsed;
	
	public static RecurringScheduleThread create(Logger logger, ProgressReportable reportable,
												long interval) {
		PeriodicProgressReporter reporter = new PeriodicProgressReporter(logger, reportable);
		return RecurringScheduleThread.newFixedRateSchedule("progress_repoter",
														reporter, interval, interval);
	}
	
	private PeriodicProgressReporter(Logger logger, ProgressReportable reportable) {
		m_reportLogger = logger;
		m_reportable = reportable;
	}

	@Override
	public void onStarted(RecurringSchedule schedule) throws Throwable {
		m_elapsed = StopWatch.start();
	}

	@Override
	public void onStopped() {
		m_elapsed.stop();
	}

	@Override
	public void perform() throws Exception {
		m_reportable.reportProgress(m_reportLogger, m_elapsed);
		m_reportLogger.info("--------------------------------------------------------------------------------------------------------------------------------");
	}
}
