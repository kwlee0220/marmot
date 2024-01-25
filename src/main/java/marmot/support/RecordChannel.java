package marmot.support;

import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;

import com.google.common.collect.Lists;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSetClosedException;
import marmot.RecordSetException;
import marmot.rset.AbstractRecordSet;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordChannel extends AbstractRecordSet {
	enum State {
		/** Consumer가 필요한 레코드가 제공되기를 기다리는 상태. */
		UNLOADED,
		/** Supplier가 생성한 레코드 블럭이 다 처리되기를 기다리는 상태.*/
		LOADED,
		/** Supplier가 더 이상의 레코드 생성이 완료되어 더 이상의 레코드 추가가 멈춘 상태. */
		SUPPLY_CLOSED,
		/** Consumer가 (오류 발생으로) 더이상 데이터를 읽지 않게된 상태 */
		CONSUMER_CLOSED,
		/** 레코드 채널이 폐쇄된 상태. */
		CLOSED };

	private final RecordSchema m_schema;
	private List<Record> m_locals;
	private int m_index;
	
	private final ReentrantLock m_lock = new ReentrantLock();
	private final Condition m_cond = m_lock.newCondition();
	@GuardedBy("m_lock") private State m_state = State.UNLOADED;
	@GuardedBy("m_lock") private List<Record> m_shareds;
	
	public RecordChannel(RecordSchema schema) {
		m_schema = schema;
		m_locals = Lists.newArrayList();
		m_index = 0;
	}
	
	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	@Override
	protected void closeInGuard() {
		m_lock.lock();
		try {
			switch ( m_state ) {
				case LOADED:
				case UNLOADED:
					m_shareds  = null;
					m_state = State.CONSUMER_CLOSED;
					m_cond.signalAll();
					break;
				case SUPPLY_CLOSED:
				case CONSUMER_CLOSED:
					m_shareds = null;
					m_state = State.CLOSED;
					m_cond.signalAll();
					break;
				default:
					break;
			}
		}
		finally {
			m_lock.unlock();
		}
	}
	
	@Override
	public boolean next(Record record) throws RecordSetException {
		checkNotClosed();
		
		while ( true ) {
			if ( m_index < m_locals.size() ) {
				record.set(m_locals.get(m_index++));
				return true;
			}
			
			m_lock.lock();
			try {
				while ( true ) {
					if ( m_state == State.LOADED ) {
						m_locals = m_shareds;
						m_index = 0;
						m_shareds = null;
						
						m_state = State.UNLOADED;
						m_cond.signalAll();
						break;
					}
					else if ( m_state == State.UNLOADED ) {
						m_cond.await();
					}
					else if ( m_state == State.SUPPLY_CLOSED ) {
						return false;
					}
					else if ( m_state == State.CLOSED ) {
						throw new RecordSetClosedException(""+this);
					}
					else if ( m_state == State.CONSUMER_CLOSED ) {
						throw new IllegalStateException("cannot call next after 'CONSUMER_CLOSED'");
					}
				}
			}
			catch ( InterruptedException e ) {
				throw new RecordSetException(e);
			}
			finally {
				m_lock.unlock();
			}
		}
	}
	
	public State supply(List<Record> records) throws InterruptedException {
		m_lock.lock();
		try {
			while ( true ) {
				switch ( m_state ) {
					case UNLOADED:
						m_shareds = records;
						m_state = State.LOADED;
						m_cond.signalAll();
						return m_state;
					case LOADED:
						m_cond.await();
						break;
					case CONSUMER_CLOSED:
						return m_state;
					case CLOSED:
						throw new RecordSetClosedException("consumer has closed the channel");
					case SUPPLY_CLOSED:
						throw new IllegalStateException("supplier has closed the channel");
				}
			}
		}
		finally {
			m_lock.unlock();
		}
	}

	public void endOfSupply() throws InterruptedException {
		m_lock.lock();
		try {
			while ( true ) {
				switch ( m_state ) {
					case UNLOADED:
						m_state = State.SUPPLY_CLOSED;
						m_cond.signalAll();
						return;
					case LOADED:
						m_cond.await();
						break;
					case CONSUMER_CLOSED:
						m_state = State.CLOSED;
						m_cond.signalAll();
						return;
					default:
						return;
				}
			}
		}
		finally {
			m_lock.unlock();
		}
	}
	
	@Override
	public String toString() {
		return String.format("channel[%s]", ""+m_state);
	}
}
