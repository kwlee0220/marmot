package marmot.optor;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.io.RecordWritable;
import marmot.optor.support.AbstractRecordSetLoader;
import marmot.rset.AbstractRecordSet;
import marmot.support.ProgressReportable;
import utils.StopWatch;
import utils.func.Try;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LoadKafkaTopic extends AbstractRecordSetLoader {
	private final String m_topic;
	private final RecordSchema m_schema;
	
	public LoadKafkaTopic(String topic, RecordSchema schema) {
		m_topic = topic;
		m_schema = schema;
		
		setLogger(LoggerFactory.getLogger(LoadKafkaTopic.class));
	}
	
	@Override
	public void initialize(MarmotCore marmot) {
		setInitialized(marmot, m_schema);
	}

	@Override
	public RecordSet load() {
		checkInitialized();
		
		return new Loaded(this);
	}
	
	@Override
	public String toString() {
		return String.format("load_kafka_topic[topic=%s]", m_topic);
	}

	static class Loaded extends AbstractRecordSet implements ProgressReportable {
		private final LoadKafkaTopic m_load;
		private final KafkaConsumer<Long, byte[]> m_consumer;
		private final RecordWritable m_writable;
		
		private Iterator<ConsumerRecord<Long, byte[]>> m_iter;
		private boolean m_finalProgressReported = false;
		private long m_count = 0;
		
		public Loaded(LoadKafkaTopic load) {
			m_load = load;
			
			m_consumer = getKafkaConsumer(m_load.m_topic);
			m_consumer.subscribe(Arrays.asList(m_load.m_topic));
			
			m_writable = RecordWritable.from(m_load.m_schema);
			m_iter = Collections.emptyIterator();
		}
	
		@Override
		public RecordSchema getRecordSchema() {
			return m_load.getRecordSchema();
		}
	
		@Override
		protected void closeInGuard() {
			Try.run(m_consumer::close);
		}
	
		@Override
		public boolean next(Record output) throws RecordSetException {
			while ( !m_iter.hasNext() ) {
				m_iter = m_consumer.poll(Long.MAX_VALUE).iterator();
			}
			
			byte[] bytes = m_iter.next().value();
			m_writable.fromBytes(bytes);
			m_writable.storeTo(output);
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
			return String.format("%s: count=%d", m_load, m_count);
		}
		
		private KafkaConsumer<Long, byte[]> getKafkaConsumer(String group) {
			Properties configs = new Properties();
			configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, m_load.m_marmot.getKafkaBrokerList());
			configs.put(ConsumerConfig.GROUP_ID_CONFIG, group);
			configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
			configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
						BytesDeserializer.class.getName());
			
			return new KafkaConsumer<>(configs);
		}
	}
}
