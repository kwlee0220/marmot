package marmot.support;

import java.util.function.Consumer;

import marmot.Record;
import marmot.RecordSchema;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface RecordConsumer extends Consumer<Record>, AutoCloseable {
	public default void open(RecordSchema schema) { }
	public default void close() { }
	public void consume(Record record);
	
	public default void accept(Record record) {
		consume(record);
	}
	
	public static RecordConsumer from(Consumer<? super Record> consumer) {
		return new RecordConsumer() {
			@Override
			public void consume(Record record) {
				consumer.accept(record);
			}
		};
	}
}
