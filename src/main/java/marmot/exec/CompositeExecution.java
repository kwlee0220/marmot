package marmot.exec;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.GuardedBy;

import utils.async.Guard;
import utils.func.CheckedRunnable;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class CompositeExecution extends AbstractMarmotExecution<CompositeAnalysis> implements CheckedRunnable {
	private final MarmotAnalysisManager m_exector;
	private final List<String> m_components;
	
	private final Guard m_guard = Guard.create();
	@GuardedBy("m_guard") private State m_state = State.RUNNING;
	@GuardedBy("m_guard") private int m_index = -1;
	@GuardedBy("m_guard") private MarmotExecution m_current;
	@GuardedBy("m_guard") private Throwable m_cause;
	@GuardedBy("m_guard") private boolean m_cancelRequested = false;
	
	CompositeExecution(MarmotAnalysisManager exector, CompositeAnalysis analysis) {
		super(FOption.of(analysis));
		
		m_exector = exector;
		m_components = analysis.getComponents();
	}

	@Override
	public int getCurrentExecutionIndex() {
		return m_guard.get(() -> m_index);
	}

	@Override
	public State getState() {
		return m_guard.get(() -> m_state);
	}

	@Override
	public Throwable getFailureCause() throws IllegalStateException {
		return m_guard.get(() -> m_cause);
	}

	@Override
	public boolean cancel() {
		m_guard.lock();
		try {
			m_cancelRequested = true;
			
			if ( m_state == State.RUNNING ) {
				m_current.cancel();
				
				while ( m_state == State.RUNNING ) {
					try {
						m_guard.awaitSignal();
					}
					catch ( InterruptedException e ) {
						return false;
					}
				}
				
				return true;
			}
			else {
				return m_state == State.CANCELLED;
			}
		}
		finally {
			m_guard.unlock();
		}
	}

	@Override
	public void waitForFinished() throws InterruptedException {
		m_guard.awaitCondition(() -> m_state != State.RUNNING).andReturn();
	}

	@Override
	public boolean waitForFinished(long timeout, TimeUnit unit) throws InterruptedException {
		Date due = new Date(System.currentTimeMillis() + unit.toMillis(timeout));

		m_guard.lock();
		try {
			while ( m_state == State.RUNNING ) {
				if ( !m_guard.awaitSignal(due) ) {
					return false;
				}
			}
			
			return true;
		}
		finally {
			m_guard.unlock();
		}
	}
	
	@Override
	public void run() throws Exception {
		m_guard.run(() -> m_state = State.RUNNING);
		
		for ( int i =0; i < m_components.size(); ++i ) {
			MarmotAnalysis analysis = m_exector.getAnalysis(m_components.get(i));
			
			m_guard.lock();
			try {
				if ( m_state == State.RUNNING ) {
					m_index = i;
					m_current = m_exector.start(analysis);
					m_guard.signalAll();
				}
				else {
					return;
				}
			}
			finally {
				m_guard.unlock();
			}
			
			try {
				m_current.waitForFinished();
				
				m_guard.lock();
				try {
					switch ( m_current.getState() ) {
						case COMPLETED:
							break;
						case FAILED:
							m_state = m_current.getState();
							m_cause = toMarmotExecutionException(m_current.getFailureCause());
							return;
						case CANCELLED:
							m_state = m_current.getState();
							return;
						default:
							throw new AssertionError();
					}
				}
				finally {
					m_guard.unlock();
				}
			}
			catch ( Exception e ) {
				Throwable cause = MarmotAnalysisManager.unwrapThrowable(e);
				String compId = m_components.get(m_index);
				String msg = String.format("%s: component[%d:%s] failed, cause=%s",
											m_analysis.get().getId(), m_index, compId, cause);
				m_guard.run(() -> {
					m_state = m_current.getState();
					m_cause = new MarmotExecutionException(msg);
				});
				
				throw e;
				
			}
		}
		
		m_guard.run(() -> m_state = State.COMPLETED);
	}
	
	private MarmotExecutionException toMarmotExecutionException(Throwable e) {
		Throwable cause = MarmotAnalysisManager.unwrapThrowable(e);
		String compId = m_components.get(m_index);
		String msg = String.format("%s: component[%d:%s] failed, cause=%s",
									m_analysis.get().getId(), m_index, compId, cause);
		return new MarmotExecutionException(msg);
	}
}