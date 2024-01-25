package marmot.exec;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.MarmotCore;
import marmot.analysis.system.SystemAnalysis;
import utils.Throwables;
import utils.async.AbstractThreadedExecution;
import utils.async.StartableExecution;
import utils.func.CheckedRunnable;
import utils.func.Try;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotAnalysisManager {
	private final static Logger s_logger = LoggerFactory.getLogger(MarmotAnalysisManager.class);
	
	private final MarmotCore m_marmot;
	private final MarmotAnalysisStore m_analysisStore;
	private final SystemAnalysisExecutor m_sysAnalExector;
	private final ScheduledExecutorService m_executor;
	private final MarmotExecutionRegistry m_registry;
	
	public MarmotAnalysisManager(MarmotCore marmot, MarmotExecutionRegistry registry,
									ScheduledExecutorService exector) {
		m_marmot = marmot;

		m_analysisStore = MarmotAnalysisStore.initialize(marmot.getHadoopConfiguration());
		m_sysAnalExector = new SystemAnalysisExecutor(marmot);
		m_executor = exector;
		m_registry = registry;
	}

	public MarmotAnalysis getAnalysis(String id) throws AnalysisNotFoundException {
		return m_analysisStore.get(id);
	}

	public List<MarmotAnalysis> getAnalysisAll() {
		return m_analysisStore.getAll();
	}

	public MarmotAnalysis findAnalysis(String id) {
		return m_analysisStore.find(id);
	}

	public CompositeAnalysis findParentAnalysis(String id) {
		return m_analysisStore.findParent(id);
	}

	public List<CompositeAnalysis> getAncestorAnalysisAll(String id) {
		return m_analysisStore.getAncestorAll(id);
	}
	
	public List<MarmotAnalysis> getDescendantAnalysisAll(String id) {
		return m_analysisStore.getDescendantAll(id);
	}

	public void addAnalysis(MarmotAnalysis analysis, boolean force) {
		m_analysisStore.add(analysis, force);
	}

	public void deleteAnalysis(String id, boolean recursive) {
		m_analysisStore.remove(id, recursive);
	}

	public void deleteAnalysisAll() {
		m_analysisStore.removeAll();
	}
	
	public MarmotExecution start(MarmotAnalysis analysis) {
		switch ( analysis.getType() ) {
			case PLAN:
				return start((PlanAnalysis)analysis);
			case COMPOSITE:
				CompositeExecution cexec = new CompositeExecution(this, (CompositeAnalysis)analysis);
				m_registry.register(cexec);
				CompletableFuture.supplyAsync(Try.lift(cexec));
				return cexec;
			case MODULE:
				return start(analysis, () -> executeModuleAnalysis((ModuleAnalysis)analysis));
			case SYSTEM:
				return start(analysis, () -> m_sysAnalExector.execute((SystemAnalysis)analysis));
			case EXTERN:
				ExternExecution eexec = new ExternExecution((ExternAnalysis)analysis, m_executor);
				CompletableFuture.supplyAsync(Try.lift(eexec));
				m_registry.register(eexec);
				return eexec;
			default:
				throw new IllegalArgumentException("unsupported Analysis type: " + analysis.getType());
		}
	}
	
	public void execute(MarmotAnalysis analysis) throws MarmotExecutionException {
		MarmotExecution exec = start(analysis);
		try {
			exec.waitForFinished();
			switch ( exec.getState() ) {
				case COMPLETED:
					break;
				case FAILED:
					throw toMarmotExecutionException(exec.getFailureCause());
				case CANCELLED:
					throw toMarmotExecutionException(new CancellationException());
				default:
					throw new AssertionError();
			}
		}
		catch ( Throwable e ) {
			throw toMarmotExecutionException(e);
		}
	}
	
	private MarmotExecution start(PlanAnalysis analysis) {
		PlanExecution planExec = m_marmot.createPlanExecution(analysis.getPlan(),
															analysis.getExecuteOptions());
		MarmotExecution exec = new AsyncMarmotExecution<>(analysis, planExec);
		
		planExec.start();
		m_registry.register(exec);
		
		return exec;
	}
	
	private <T> MarmotExecution start(MarmotAnalysis analysis, CheckedRunnable task) {
		try {
			StartableExecution<Void> async = new AbstractThreadedExecution<Void>() {
				@Override
				protected Void executeWork() throws Exception {
					try {
						task.run();
						return null;
					}
					catch ( Throwable e ) {
						Throwables.throwIfInstanceOf(e, Exception.class);
						throw Throwables.toRuntimeException(e);
					}
				}
			};
			async.start();
			
			MarmotExecution exec = new AsyncMarmotExecution<>(analysis, async);
			m_registry.register(exec);
			
			return exec;
		}
		catch ( CancellationException e ) {
			throw e;
		}
		catch ( Throwable e ) {
			Throwable cause = unwrapThrowable(e);
			Throwables.throwIfInstanceOf(cause, MarmotExecutionException.class);
			throw new MarmotExecutionException(cause);
		}
	}
	
	private MarmotExecutionException toMarmotExecutionException(Throwable e) {
		Throwable cause = unwrapThrowable(e);
		Throwables.throwIfInstanceOf(cause, MarmotExecutionException.class);
		return new MarmotExecutionException(cause);
	}

	private void executeModuleAnalysis(ModuleAnalysis analysis) throws MarmotExecutionException {
		m_marmot.executeProcess(analysis.getModuleId(), analysis.getArguments());
	}
	
	static Throwable unwrapThrowable(Throwable e) {
		Throwable cause = Throwables.unwrapThrowable(e);
		if ( cause instanceof MarmotExecutionException ) {
			Throwable c2 = ((MarmotExecutionException)cause).getCause();
			if ( c2 != null ) {
				cause = c2;
			}
		}
		
		return cause;
	}
}
