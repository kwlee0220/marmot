package marmot.kafka;

import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import marmot.ExecutePlanOptions;
import marmot.MarmotCore;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSetException;
import marmot.exec.PlanExecution;
import marmot.io.RecordWritable;
import marmot.optor.RecordSetConsumer;
import marmot.optor.RecordSetOperator;
import marmot.optor.StoreIntoKafkaTopic;
import marmot.rset.AbstractRecordSet;
import marmot.support.ProgressReportable;
import marmot.support.RecordSetOperatorChain;
import utils.StopWatch;
import utils.func.Try;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class KafkaTopicRecordSet extends AbstractRecordSet
								implements ProgressReportable {
	private final long TIMEOUT = 1 * 1000;
	
	private final MarmotCore m_marmot;
	private final String m_topic;
	private final Plan m_plan;
	private final PlanExecution m_pexec;
	private final KafkaConsumer<Long, byte[]> m_consumer;
	private Iterator<ConsumerRecord<Long, byte[]>> m_iter;
	private boolean m_finalProgressReported = false;
	private long m_count = 0;
	private long m_timeout;
	
	public KafkaTopicRecordSet(MarmotCore marmot, Plan plan, String topic) {
		setLogger(LoggerFactory.getLogger(KafkaTopicRecordSet.class));
		
		m_marmot = marmot;
		m_topic = topic;
		
		KafkaUtils.createKafkaTopic(marmot.getZooKeeperHosts(), topic, 1, 1, true);
		
		m_consumer = getKafkaConsumer(marmot.getKafkaBrokerList(), topic);
		m_consumer.subscribe(Lists.newArrayList(topic));
		
		m_iter = Collections.emptyIterator();
		m_timeout = Long.MAX_VALUE;
		
		m_plan = adjustPlanForKafkaTopic(plan, topic);
		m_pexec = marmot.createPlanExecution(m_plan, ExecutePlanOptions.DEFAULT);
		m_pexec.start();
		m_pexec.whenFinishedAsync(result -> {
			m_timeout = TIMEOUT;
			m_consumer.wakeup();
		});
	}
	
	@Override
	public RecordSchema getRecordSchema() {
		return m_marmot.getOutputRecordSchema(m_plan);
	}

	@Override
	protected void closeInGuard() {
		Try.run(this::_close);
	}

	@Override
	public boolean next(Record output) throws RecordSetException {
		checkNotClosed();
		
		if ( m_iter == null ) {
			return false;
		}
		
		while ( !m_iter.hasNext() ) {
			ConsumerRecords<Long, byte[]> fetcheds;
			try {
				fetcheds = m_consumer.poll(m_timeout);
				if ( fetcheds.isEmpty() ) {
					_close();
					
					return false;
				}
				m_iter = fetcheds.iterator();
			}
			catch ( WakeupException expected ) { }
		}
		
		RecordWritable.fromBytes(m_iter.next().value(), output);
		++m_count;
		
		return true;
	}
	
	@Override
	public void reportProgress(Logger logger, StopWatch elapsed) {
		if ( !isClosed() || !m_finalProgressReported ) {
			logger.info("report: [{}]{}", isClosed() ? "C": "O", toString());
			
			if ( isClosed() ) {
				m_finalProgressReported = true;
			}
		}
	}
	
	@Override
	public String toString() {
		return String.format("rset_kafka[topic=%s]: count=%d", m_topic, m_count);
	}
	
	private KafkaConsumer<Long, byte[]> getKafkaConsumer(String brokerList,
																String group) {
		Properties configs = new Properties();
		configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
		configs.put(ConsumerConfig.GROUP_ID_CONFIG, group);
		configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
									ByteArrayDeserializer.class.getName());
		
		return new KafkaConsumer<>(configs);
	}
	
	private synchronized void _close() {
		if ( m_iter != null ) {
			m_iter = null;
			Try.run(() -> m_pexec.cancel(true));
			Try.run(m_consumer::close);
			KafkaUtils.deleteKafkaTopic(m_marmot.getZooKeeperHosts(), m_topic);
		}
	}

	private Plan adjustPlanForKafkaTopic(Plan plan, String topic) {
		RecordSetOperatorChain chain = RecordSetOperatorChain.from(m_marmot, plan);
		if ( chain.length() == 0 ) {
			throw new IllegalArgumentException("Plan is empty");
		}
		
		// Plan의 마지막 연산이 'StoreIntoKafkaTopic'가 아닌 경우
		// 강제로 'StoreIntoKafkaTopic' 연산을 추가시킨다.
		// 만일 마지막 연산이 다른 RecordSetConsumer인 경우이거나, 다른 데이터세트의 이름으로 저장하는
		// 경우라면 예외를 발생시킨다.
		//
		RecordSetOperator last = chain.getLast();
		if ( last instanceof RecordSetConsumer ) {
			throw new IllegalArgumentException("Plan does not store into target dataset: last=" + last);
		}
		else {
			StoreIntoKafkaTopic store = new StoreIntoKafkaTopic(topic);
			chain.add(store);
		}
		
		return chain.toPlan(plan.getName());
	}
}
