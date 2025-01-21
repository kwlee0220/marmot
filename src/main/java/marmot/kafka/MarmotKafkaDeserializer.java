package marmot.kafka;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import utils.KeyValue;

import marmot.RecordSetException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotKafkaDeserializer implements Deserializer<KeyValue<String,byte[]>> {
	@Override public void configure(Map<String, ?> configs, boolean isKey) { }
	@Override public void close() { }

	@Override
	public KeyValue<String,byte[]> deserialize(String topic, byte[] bytes) {
		try ( DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes)) ) {
			String channel = dis.readUTF();
			int length = dis.readInt();
			byte[] buf = new byte[length];
			dis.readFully(buf);
			
			return KeyValue.of(channel, buf);
		}
		catch ( IOException e ) {
			throw new RecordSetException("fails to deserialize record", e);
		}
	}
}
