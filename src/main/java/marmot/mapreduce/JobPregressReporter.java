package marmot.mapreduce;

import java.util.List;

import org.apache.hadoop.mapreduce.Job;

import com.google.common.collect.Lists;

import utils.rx.ProgressReporter;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class JobPregressReporter implements ProgressReporter<String>, Runnable {
	private final Job m_job;
	private final List<Stage> m_stages = Lists.newArrayList();
	private final BehaviorSubject<String> m_subject = BehaviorSubject.create();
	
	static class MapReduceJobProgress {
		private final String m_name;
		private final int m_stageNo;
		private float m_mapProgress;
		private float m_reduceProgress;
		
		MapReduceJobProgress(String name, int stageNo, float mapProgress,
								float reduceProgress) {
			m_name = name;
			m_stageNo = stageNo;
			m_mapProgress = mapProgress;
			m_reduceProgress = reduceProgress;
		}
		
		@Override
		public String toString() {
			return String.format("%s:%d:(%.1f%%,%.1f%%)", m_name, m_stageNo,
									m_mapProgress, m_reduceProgress);
		}
	};
	
	JobPregressReporter(Job job) {
		m_job = job;
	}

	@Override
	public Observable<String> getProgressObservable() {
		return m_subject;
	}

	@Override
	public void run() {
//		ScheduledExecutorService executor = Executors.newScheduledThreadPool(5);
//		ScheduledFuture<?> future = executor.scheduleAtFixedRate(() -> {
//			JobStatus status = m_job.getStatus();
//			status.getState();
//			float mprg = status.getMapProgress();
//			float rprg = status.getReduceProgress();
//			
//			MapReduceJobProgress progress = new MapReduceJobProgress(m_job.getJobName(),
//															m_stages.size(), mprg, rprg);
//		}, 10, 10, TimeUnit.SECONDS);
	}
}