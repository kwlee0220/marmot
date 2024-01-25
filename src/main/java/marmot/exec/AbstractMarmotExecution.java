package marmot.exec;

import java.time.Duration;
import java.time.LocalDateTime;

import utils.LocalDateTimes;
import utils.UnitUtils;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AbstractMarmotExecution<T extends MarmotAnalysis> implements MarmotExecution {
	private final String m_id;
	protected final FOption<T> m_analysis;
	protected final long m_startedTime;
	
	protected volatile long m_finishedTime = -1;
	private volatile Duration m_maxRunningTime = Duration.ofDays(1);
	private volatile Duration m_retentionTime = Duration.ofDays(1);
	
	protected AbstractMarmotExecution(String id, FOption<T> analysis) {
		m_id = id;
		m_analysis = analysis;
		m_startedTime = System.currentTimeMillis();
	}
	
	protected AbstractMarmotExecution(FOption<T> analysis) {
		m_id = "" + System.identityHashCode(this);
		m_analysis = analysis;
		m_startedTime = System.currentTimeMillis();
	}

	@Override
	public String getId() {
		return m_id;
	}

	@Override
	public FOption<MarmotAnalysis> getMarmotAnalysis() {
		return m_analysis.cast(MarmotAnalysis.class);
	}

	@Override
	public int getCurrentExecutionIndex() {
		return 0;
	}

	@Override
	public long getStartedTime() {
		return m_startedTime;
	}

	@Override
	public long getFinishedTime() {
		return m_finishedTime;
	}

	@Override
	public Duration getMaximumRunningTime() {
		return m_maxRunningTime;
	}

	@Override
	public void setMaximumRunningTime(Duration dur) {
		m_maxRunningTime = dur;
	}

	@Override
	public Duration getRetentionTime() {
		return m_retentionTime;
	}

	@Override
	public void setRetentionTime(Duration dur) {
		m_retentionTime = dur;
	}
	
	@Override
	public String toString() {
		State state = getState();
		
		String analyStr = m_analysis.map(anal -> String.format(", %s[%s]", anal.getType(), anal.getId()))
									.getOrElse("");
		
		String failedCause = "";
		if ( state == State.FAILED ) {
			failedCause = String.format(" (cause=%s)", getFailureCause());
		}
		
		long elapsed = ( state == State.RUNNING )
					? System.currentTimeMillis() - m_startedTime
					: m_finishedTime - m_startedTime;
		String elapsedStr = UnitUtils.toSecondString(elapsed);

		LocalDateTime startedStr = LocalDateTimes.fromUtcMillis(m_startedTime);
		return String.format("%10s: %9s%s%s, started=%s, elapsed=%s", getId(), state,
							failedCause, analyStr, startedStr, elapsedStr);
	}
}
