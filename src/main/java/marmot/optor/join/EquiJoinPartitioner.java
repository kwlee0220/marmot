package marmot.optor.join;

import java.util.Objects;

import org.apache.hadoop.mapreduce.Partitioner;

import marmot.io.RecordWritable;
import marmot.mapreduce.MarmotMapOutputKey;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class EquiJoinPartitioner extends Partitioner<MarmotMapOutputKey,RecordWritable> {
	@Override
	public int getPartition(MarmotMapOutputKey key, RecordWritable value, int numPartitions) {
		Object[] joinKeyValues = key.getValueAll();
		return (Objects.hash(joinKeyValues) & Integer.MAX_VALUE) % numPartitions;
	}
}