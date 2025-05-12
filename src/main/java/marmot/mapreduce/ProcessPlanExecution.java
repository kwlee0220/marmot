package marmot.mapreduce;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CancellationException;

import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.Throwables;
import utils.async.AbstractThreadedExecution;
import utils.async.CancellableWork;
import utils.async.Guard;
import utils.io.IOUtils;

import marmot.ExecutePlanOptions;
import marmot.MarmotInternalException;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.exec.PlanExecution;
import marmot.proto.service.ExecutePlanRequest;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class ProcessPlanExecution extends PlanExecution
							implements CancellableWork {
	private final static Logger s_logger = LoggerFactory.getLogger(ProcessPlanExecution.class);
	
	private final List<String> m_command;
	private final Plan m_plan;
	private ExecutePlanOptions m_opts = ExecutePlanOptions.DEFAULT;
	private final RecordSchema m_outSchema;
	
	private final Guard m_guard = Guard.create();
	@GuardedBy("m_guard") private Process m_proc = null;
	
	ProcessPlanExecution(List<String> command, Plan plan, RecordSchema outSchema) {
		m_command = command;
		m_plan = plan;
		m_outSchema = outSchema;
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_outSchema;
	}

	@Override
	public void setDisableLocalExecution(boolean flag) {
		m_opts = m_opts.disableLocalExecution(flag);
	}

	@Override
	public void setMapOutputCompressCodec(String codec) {
		// not implemented yet
	}

	@Override
	protected Void executeWork() throws CancellationException, Exception {
		int exitCode = forkExec();
		if ( exitCode < 0 ) {
			throw new MarmotInternalException("fails to launch PlanExecutorMain: "
											+ "exitCode=" + exitCode);
		}
		
		return null;
	}
	
	@Override
	public boolean cancelWork() {
		return m_guard.get(() -> {
			if ( m_proc != null ) {
				IOUtils.closeQuietly(m_proc.getOutputStream());
				return true;
			}
			else {
				// forkExec() 수행 이전에 cancel이 호출된 경우.
				return true;
			}
		});
	}
	
	private int forkExec() {
		try {
			Process proc = m_guard.getChecked(() -> {
				switch ( getState() ) {
					case RUNNING:
						return createAndStartProcess();
					case CANCELLING:
						return null;
					default:
						throw new IllegalStateException("state=" + getState());
				}
			});
			return proc.waitFor();
		}
		catch ( Exception e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			Throwables.toRuntimeException(cause);
			
			return -1;
		}
	}
	
	private Process createAndStartProcess() throws IOException {
		m_proc = new ProcessBuilder(m_command).start();
		
		IOUtils.copy(m_proc.getInputStream(), System.out)
				.closeInputStreamOnFinished(true).start();
		IOUtils.copy(m_proc.getErrorStream(), System.err)
				.closeInputStreamOnFinished(true).start();
		
		new PlanStdinSupplier(m_proc.getOutputStream(), m_plan, m_opts).start();
		
		return m_proc;
	}
	
	private static final class PlanStdinSupplier extends AbstractThreadedExecution<Void>
												implements CancellableWork {
		private final OutputStream m_os;
		private final Plan m_plan;
		private final ExecutePlanOptions m_opts;
		
		private PlanStdinSupplier(OutputStream os, Plan plan, ExecutePlanOptions opts) {
			m_os = os;
			m_plan = plan;
			m_opts = opts;
		}

		@Override
		public Void executeWork() throws CancellationException, Exception {
			ExecutePlanRequest req = ExecutePlanRequest.newBuilder()
														.setPlan(m_plan.toProto())
														.setOptions(m_opts.toProto())
														.build();
			req.writeDelimitedTo(m_os);
			m_os.flush();
			
			s_logger.debug("plan sent to PlanExecutionProcess: plan={}", m_plan);
			
			return null;
		}

		@Override
		public boolean cancelWork() {
			IOUtils.closeQuietly(m_os);
			return true;
		}
	}
}
