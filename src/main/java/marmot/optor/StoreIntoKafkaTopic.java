package marmot.optor;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.kafka.MarmotKafkaSerializer;
import marmot.mapreduce.MapReduceJobConfigurer;
import marmot.mapreduce.MapReduceTerminal;
import marmot.mapreduce.MarmotMRContext;
import marmot.mapreduce.MarmotMRContextAware;
import marmot.optor.support.AbstractRecordSetConsumer;
import marmot.proto.optor.StoreIntoKafkaTopicProto;
import marmot.support.PBSerializable;
import marmot.support.ProgressReportable;
import utils.StopWatch;
import utils.Utilities;
import utils.func.FOption;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StoreIntoKafkaTopic extends AbstractRecordSetConsumer
								implements MapReduceTerminal, MapReduceJobConfigurer,
											MarmotMRContextAware, ProgressReportable,
											PBSerializable<StoreIntoKafkaTopicProto> {
	private static final int INIT_BUFFER_SIZE = 1;
	private static final int BUFFER_SIZE = 128;
	
	private final String m_topic;
	
	private String m_clientId;
	private volatile RecordSet m_inputRSet;
	private int m_count;
	
	public StoreIntoKafkaTopic(String topic) {
		Utilities.checkNotNullArgument(topic, "topic is null");

		m_topic = topic;
		m_clientId = topic;
		
		setLogger(LoggerFactory.getLogger(StoreIntoKafkaTopic.class));
	}
	
	@Override
	public void setMarmotMRContext(MarmotMRContext context) {
		m_clientId = String.format("%s_%05d", m_topic, context.getTaskOrdinal());
	}
	
	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		setInitialized(marmot, inputSchema);
	}
	
	@Override
	public void configure(Job job) {
		checkInitialized();

		job.setOutputFormatClass(NullOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);
	}

	@Override
	public void consume(RecordSet rset) {
		checkInitialized();
		Utilities.checkNotNullArgument(rset, "rset is null");
		
		m_count = 0;
		
		RecordSchema schema = rset.getRecordSchema();
		KafkaProducer<Long,Record> producer = createProducer(m_clientId, schema);
		try {
			FStream<ProducerRecord<Long,Record>> records = rset.fstream()
											.map(r -> new ProducerRecord<>(m_topic, r));
			sendInitialBlock(producer, records);
			
			records.buffer(BUFFER_SIZE, BUFFER_SIZE)
					.forEach(block -> {
						sendBlock(producer, block);
						m_count += block.size();
					});
		}
		finally {
			m_inputRSet = null;
			producer.close();
			rset.closeQuietly();
		}
	}
	
	@Override
	public void reportProgress(Logger logger, StopWatch elapsed) {
		RecordSet rset = m_inputRSet;
		if ( rset != null && rset instanceof ProgressReportable ) {
			((ProgressReportable)rset).reportProgress(logger, elapsed);
		}
		
		long velo = Math.round(m_count / elapsed.getElapsedInFloatingSeconds());
		logger.info("report: {}, count={} elapsed={} velo={}/s",
							this, m_count, elapsed.getElapsedMillisString(), velo);
	}
	
	@Override
	public String toString() {
		return String.format("store_into_kafka[topic=%s, client=%s]",
								m_topic, m_clientId);
	}

	public static StoreIntoKafkaTopic fromProto(StoreIntoKafkaTopicProto proto) {
		return new StoreIntoKafkaTopic(proto.getTopic());
	}

	@Override
	public StoreIntoKafkaTopicProto toProto() {
		return StoreIntoKafkaTopicProto.newBuilder()
										.setTopic(m_topic)
										.build();
	}
	
	private KafkaProducer<Long,Record> createProducer(String id, RecordSchema schema) {
		Properties configs = new Properties();
		configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, m_marmot.getKafkaBrokerList());
		configs.put(ProducerConfig.CLIENT_ID_CONFIG, id);
		configs.put(ProducerConfig.ACKS_CONFIG, "all");
		configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MarmotKafkaSerializer.class.getName());
		configs.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		
		return new KafkaProducer<>(configs);
	}
	
	private void sendInitialBlock(KafkaProducer<Long,Record> producer,
									FStream<ProducerRecord<Long,Record>> records) {
		List<ProducerRecord<Long,Record>> block = new ArrayList<>(INIT_BUFFER_SIZE);
		for ( int i =0; i < INIT_BUFFER_SIZE; ++i ) {
			FOption<ProducerRecord<Long,Record>> rec = records.next();
			if ( rec.isPresent() ) {
				block.add(rec.get());
			}
			else {
				break;
			}
		}
		
		if ( !block.isEmpty() ) {
			sendBlock(producer, block);
		}
	}
	
	private void sendBlock(KafkaProducer<Long,Record> producer,
							List<ProducerRecord<Long,Record>> block) {
		CountDownLatch countDown = new CountDownLatch(block.size());
		
		for ( ProducerRecord<Long,Record> r: block ) {
			producer.send(r, (meta,fault) -> {
				countDown.countDown();
			});
		}
		try {
			if ( !countDown.await(BUFFER_SIZE, TimeUnit.SECONDS) ) {
				throw new RecordSetException("fails to send record: topic=" + m_topic);
			}
		}
		catch ( InterruptedException e ) {
			throw new RecordSetException("fails to send record: topic=" + m_topic);
		}
	}
}
