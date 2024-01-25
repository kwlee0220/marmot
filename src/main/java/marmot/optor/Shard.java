package marmot.optor;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import com.google.common.base.Preconditions;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.io.RecordWritable;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.proto.optor.ShardProto;
import marmot.support.PBSerializable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Shard extends AbstractRecordSetFunction implements PBSerializable<ShardProto> {
	private final int m_nparts;
	
	private Shard(int nparts) {
		Preconditions.checkArgument(nparts >= 1, "invalid partition-count: count=" + nparts);
		
		m_nparts = nparts;
	}

//	@Override
//	public boolean isMapReduceRequired() {
//		return true;
//	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		setInitialized(marmot, inputSchema, inputSchema);
	}

	@Override
	public MapReduceJoint getMapReduceJoint(RecordSchema inputSchema) {
		MapReduceJoint joint = MapReduceJoint.create()
									.addReducer(Nop.get())
//									.mapOutputKeyClass(GroupKeyValue.class)
									.setReducerCount(m_nparts);
		if ( m_nparts > 1 ) {
			joint.setPartitionerClass(RoundRobinPartitioner.class);
		}
		
		return joint;
	}

	@Override
	public RecordSet apply(RecordSet input) {
//		throw new RecordSetException("Should not be called");
		return input;
	}
	
	@Override
	public String toString() {
		return String.format("shard[nparts=%d]", m_nparts);
	}
	
	public static Shard fromProto(ShardProto proto) {
		return new Shard(proto.getPartCount());
	}

	@Override
	public ShardProto toProto() {
		return ShardProto.newBuilder()
							.setPartCount(m_nparts)
							.build();
	}
	
	static class RoundRobinPartitioner extends Partitioner<NullWritable, RecordWritable> {
		private int m_pointer = 0;
		
		@Override
		public int getPartition(NullWritable key, RecordWritable value, int numPartitions) {
			int partNo = m_pointer;
			m_pointer = (m_pointer + 1) % numPartitions;
			
			return partNo;
		}
	}
}
