package marmot.exec;

import java.util.concurrent.TimeUnit;

import utils.async.AsyncResult;
import utils.async.Execution;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AsyncMarmotExecution<T extends MarmotAnalysis> extends AbstractMarmotExecution<T> {
	private final Execution<Void> m_exec;
	
	public AsyncMarmotExecution(T analysis, Execution<Void> exec) {
		super("" + System.identityHashCode(exec), FOption.of(analysis));
		
		m_exec = exec;
		m_exec.whenFinishedAsync(result -> m_finishedTime = System.currentTimeMillis());
	}
	
	public AsyncMarmotExecution(Execution<Void> exec) {
		super("" + System.identityHashCode(exec), FOption.empty());
		
		m_exec = exec;
		m_exec.whenFinishedAsync(result -> m_finishedTime = System.currentTimeMillis());
	}
	
	public Execution<Void> getExecution() {
		return m_exec;
	}

	@Override
	public State getState() {
		switch ( m_exec.getState() ) {
			case STARTING:
			case RUNNING:
			case CANCELLING:
				return State.RUNNING;
			case COMPLETED:
				return State.COMPLETED;
			case FAILED:
				return State.FAILED;
			case CANCELLED:
				return State.CANCELLED;
			default:
				throw new IllegalStateException();
		}
	}

	@Override
	public Throwable getFailureCause() throws IllegalStateException {
		AsyncResult<Void> oresult = m_exec.poll();
		if ( !oresult.isRunning() ) {
			return oresult.getFailureCause();
		}
		else {
			throw new IllegalStateException("not failed state: state=" + getState());
		}
	}

	@Override
	public boolean cancel() {
		return m_exec.cancel(true);
	}

	@Override
	public void waitForFinished() throws InterruptedException {
		m_exec.waitForFinished();
	}

	@Override
	public boolean waitForFinished(long timeout, TimeUnit unit) throws InterruptedException {
		return !m_exec.waitForFinished(timeout, unit).isRunning();
	}
}
