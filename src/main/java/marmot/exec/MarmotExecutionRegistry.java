package marmot.exec;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import utils.async.Guard;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotExecutionRegistry {
	private static final Logger s_logger = LoggerFactory.getLogger(MarmotExecutionRegistry.class);
	
	private static final int REFRESH_MINUTES = 1;
	
	private final ScheduledFuture<?> m_schedule;
	private final Guard m_guard = Guard.create();
	@GuardedBy("guard") private final Map<String, MarmotExecution> m_registry = new LinkedHashMap<>();
	
	public MarmotExecutionRegistry(ScheduledExecutorService executor) {
		m_schedule = executor.scheduleAtFixedRate(this::refresh, REFRESH_MINUTES, REFRESH_MINUTES,
													TimeUnit.MINUTES);
	}
	
	public void shutdown() {
		m_schedule.cancel(true);
	}
	
	public MarmotExecution getMarmotExecution(String id) throws ExecutionNotFoundException {
		m_guard.lock();
		try {
			MarmotExecution exec = m_registry.get(id);
			if ( exec == null ) {
				throw new ExecutionNotFoundException(id);
			}
			
			return exec;
		}
		finally {
			m_guard.unlock();
		}
	}
	
	public List<MarmotExecution> getMarmotExecutionAll() {
		return m_guard.get(() -> Lists.newArrayList(m_registry.values()));
	}
	
	public void register(MarmotExecution exec) {
		m_guard.lock();
		try {
			String id = exec.getId();
			
			MarmotExecution old = m_registry.put(id, exec);
			if ( old != null ) {
				m_registry.put(id, old);
				
				throw new IllegalArgumentException("already registered execution: id=" + id);
			}
			
			s_logger.debug("registered: id={}, exec={}", id, exec);
		}
		finally {
			m_guard.unlock();
		}
	}
	
	public MarmotExecution unregister(String id) {
		m_guard.lock();
		try {
			MarmotExecution exec = m_registry.remove(id);
			if ( s_logger.isDebugEnabled() && exec != null ) {
				s_logger.debug("unregistered: id={}, exec={}", id, exec);
			}
			
			return exec;
		}
		finally {
			m_guard.unlock();
		}
	}
	
	private void refresh() {
		long now = System.currentTimeMillis();
		List<MarmotExecution> tooLongExecs = Lists.newArrayList();
		List<MarmotExecution> tooOld = Lists.newArrayList();
		
		int ncancelleds = 0;
		int nforgottens = 0;
		s_logger.info("starting MarmotExecutionManager referesh...");
		
		m_guard.lock();
		try {
			for ( MarmotExecution exec: m_registry.values() ) {
				if ( exec.isRunning() ) {
					Duration mrt = exec.getMaximumRunningTime();
					if ( mrt != null && now >= (exec.getStartedTime() + mrt.toMillis()) ) {
						tooLongExecs.add(exec);
					}
				}
				else {
					if ( now >= (exec.getFinishedTime() + exec.getRetentionTime().toMillis()) ) {
						tooOld.add(exec);
					}
				}
			}
			
			for ( MarmotExecution exec: tooOld ) {
				unregister(exec.getId());
				
				++nforgottens;
				s_logger.info("remove too old execution: id={}", exec.getId());
			}
		}
		finally {
			m_guard.unlock();
		}
		
		for ( MarmotExecution exec: tooLongExecs ) {
			s_logger.info("cancelling too long execution: id={}", exec.getId());
			if ( exec.cancel() ) {
				++ncancelleds;
			}
		}
		
		s_logger.info("done: MarmotExecutionManager referesh: cancelleds={}, forgottens={}",
						ncancelleds, nforgottens);
	}
}
