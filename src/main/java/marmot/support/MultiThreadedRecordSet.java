package marmot.support;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.rset.AbstractRecordSet;
import utils.func.UncheckedConsumer;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MultiThreadedRecordSet extends AbstractRecordSet {
	private static final Logger s_logger = LoggerFactory.getLogger(MultiThreadedRecordSet.class);
	private static final int DEFAULT_QUEUE_LENGTH = 64;
	private static final int DEFAULT_WORKER_COUNT = 4;
	
	private final RecordSchema m_outSchema;
	@GuardedBy("this") private final List<Worker> m_workers;
	private final BlockingQueue<Record> m_inQ = new ArrayBlockingQueue<>(DEFAULT_QUEUE_LENGTH);
	private final BlockingQueue<Record> m_outQ = new ArrayBlockingQueue<>(DEFAULT_QUEUE_LENGTH);
	private final AtomicBoolean m_noMoreInput = new AtomicBoolean(false);
	private final AtomicBoolean m_stopRequested = new AtomicBoolean(false);
	
	public MultiThreadedRecordSet(RecordSet rset, RecordSchema outSchema,
									Function<Record,Record> work) {
		m_outSchema = outSchema;
		m_workers = IntStream.range(0, DEFAULT_WORKER_COUNT)
							.mapToObj(idx -> new Worker(work))
							.collect(Collectors.toList());
		
		ForkJoinPool pool = ForkJoinPool.commonPool();
		pool.execute(() -> {
			try {
				rset.forEach(UncheckedConsumer.ignore(m_inQ::put));
			}
			catch ( Exception e ) {
				return;
			}
			finally {
				m_noMoreInput.set(true);
				if ( s_logger.isDebugEnabled() ) {
					s_logger.debug("done: input RecordSet push");
				}
			}
		});
		m_workers.forEach(worker -> pool.execute(worker));
	}

	@Override
	protected void closeInGuard() {
		m_stopRequested.set(true);
		synchronized ( this ) {
			if ( m_workers.size() > 0 ) {
				try {
					this.wait();
				}
				catch ( InterruptedException ignored ) { }
			}
		}
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_outSchema;
	}

	int m_count = 0;
	
	@Override
	public Record nextCopy() throws RecordSetException {
		checkNotClosed();
		
		try {
			while ( true ) {
				Record polled =  m_outQ.poll(500, TimeUnit.MILLISECONDS);
				if ( polled != null ) {
					return polled;
				}
				else if ( m_noMoreInput.get() ) {
					synchronized ( this ) {
						if ( m_workers.size() == 0 ) {
							return null;
						}
					}
				}
			}
		}
		catch ( InterruptedException e ) {
			return null;
		}
	}

	class Worker implements Runnable {
		private Function<Record,Record> m_work;
		
		Worker(Function<Record,Record> work) {
			m_work = work;
		}
		
		private void cleanup() {
			synchronized ( MultiThreadedRecordSet.this ) {
				m_workers.remove(this);
				MultiThreadedRecordSet.this.notifyAll();
			}
		}

		@Override
		public void run() {
			while ( true ) {
				if ( m_stopRequested.get() ) {
					if ( s_logger.isDebugEnabled() ) {
						s_logger.debug("stop requested");
					}
					cleanup();
					return;
				}
				
				try {
					Record input = m_inQ.poll(500, TimeUnit.MILLISECONDS);
					if ( input != null ) {
						try {
							m_outQ.put(m_work.apply(input));
						}
						catch ( Exception e ) {
							s_logger.warn(""+e);
						}
					}
					else if ( m_inQ.size() == 0 && m_noMoreInput.get() ) {
						if ( s_logger.isDebugEnabled() ) {
							s_logger.debug("no more input record so worker is finished");
						}
						cleanup();
						return;
					}
				}
				catch ( InterruptedException e ) {
					cleanup();
					return;
				}
			}
		}
	}
}
