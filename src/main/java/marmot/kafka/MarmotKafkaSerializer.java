package marmot.kafka;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import org.hsqldb.lib.DataOutputStream;

import marmot.Record;
import marmot.RecordSetException;
import marmot.io.RecordWritable;
import utils.func.KeyValue;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotKafkaSerializer implements Serializer<KeyValue<String,Record>> {
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) { }

	@Override
	public byte[] serialize(String topic, KeyValue<String,Record> data) {
		try ( ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(baos)); ) {
			dos.writeUTF(data.key());
			byte[] bytes = RecordWritable.from(data.value()).toBytes();
			dos.writeInt(bytes.length);
			dos.write(bytes);
			
			return baos.toByteArray();
		}
		catch ( IOException e ) {
			throw new RecordSetException("fails to serialize record", e);
		}
	}

	@Override
	public void close() { }
}
