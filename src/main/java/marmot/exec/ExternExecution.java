package marmot.exec;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.async.Guard;
import utils.func.CheckedRunnable;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class ExternExecution extends AbstractMarmotExecution<ExternAnalysis> implements CheckedRunnable {
	private static final Logger s_logger = LoggerFactory.getLogger(ExternExecution.class);
	
	private final Executor m_exector;
	private final Guard m_guard = Guard.create();
	@GuardedBy("m_guard") private Process m_proc;
	@GuardedBy("m_guard") private State m_state = State.RUNNING;
	@GuardedBy("m_guard") private Integer m_exitCode;
	
	ExternExecution(ExternAnalysis analysis, Executor exector) {
		super(FOption.of(analysis));
		
		m_exector = exector;
	}

	@Override
	public void run() throws Throwable {
		ExternAnalysis anal = m_analysis.get();
		
		List<String> command = new ArrayList<>();
		command.add(anal.getExecPath());
		command.addAll(anal.getArguments());
		
		s_logger.info("start an ExternAnalysis: {}", anal);
		
		Process proc = new ProcessBuilder(command)
//							.redirectOutput(new File("/dev/null"))
							.inheritIO()
							.start();
//		CompletableFuture.runAsync(new OutputSuppressor(proc.getInputStream()), m_exector);
//		CompletableFuture.runAsync(new OutputSuppressor(proc.getErrorStream()), m_exector);

		m_guard.runAndSignalAll(() -> m_proc = proc);
	}

	@Override
	public State getState() {
		m_guard.lock();
		try {
			if ( m_state != null && m_state != State.RUNNING ) {
				return m_state;
			}
			
			if ( m_proc == null ) {
				return m_state;
			}
			
			if ( m_proc.isAlive() ) {
				return State.RUNNING;
			}
			
			if ( m_exitCode == null ) {
				m_exitCode = m_proc.exitValue();
			}
			if (m_exitCode >= 0 ) {
				return m_state = State.COMPLETED;
			}
			else {
				return m_state = State.FAILED;
			}
		}
		finally {
			m_guard.unlock();
		}
	}

	@Override
	public Throwable getFailureCause() throws IllegalStateException {
		return m_guard.get(() -> {
			State state = getState();
			if ( state != State.FAILED ) {
				throw new IllegalStateException("not failed: state=" + state);
			}
			
			return new MarmotExecutionException("Extern process is failed: exitCode=" + m_exitCode);
		});
	}

	@Override
	public boolean cancel() {
		m_proc.destroy();
		return !m_proc.isAlive();
	}

	@Override
	public void waitForFinished() throws InterruptedException {
		m_guard.awaitUntil(() -> m_proc != null);
		m_proc.waitFor();
	}

	@Override
	public boolean waitForFinished(long timeout, TimeUnit unit) throws InterruptedException {
		m_guard.awaitUntil(() -> m_proc != null);
		return m_proc.waitFor(timeout, unit);
	}
	
	private static class OutputSuppressor implements Runnable {
		private final InputStream m_input;
		
		OutputSuppressor(InputStream input) {
			m_input = input;
		}

		@Override
		public void run() {
			byte[] buf = new byte[4096];
			
			try {
				while ( m_input.read(buf) >= 0 ) { }
			}
			catch ( IOException ignored ) { }
		}
		
	}
}