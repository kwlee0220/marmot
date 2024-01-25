package marmot.optor;

import com.google.common.collect.MinMaxPriorityQueue;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.io.MultiColumnKey;
import marmot.io.RecordKey;
import marmot.optor.rset.SingleInputRecordSet;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.proto.optor.PickTopKProto;
import marmot.support.PBSerializable;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PickTopK extends AbstractRecordSetFunction
						implements PBSerializable<PickTopKProto> {
	private final MultiColumnKey m_sortKeyCols;
	private final int m_topK;
	
	private PickTopK(MultiColumnKey compareKeyCols, int k) {
		m_sortKeyCols = compareKeyCols;
		m_topK = k;
	}
	
	public MultiColumnKey getCompareColumns() {
		return m_sortKeyCols;
	}
	
	public int getK() {
		return m_topK;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		setInitialized(marmot, inputSchema, inputSchema);
	}

	@Override
	public MapReduceJoint getMapReduceJoint(RecordSchema inputSchema) {
		return MapReduceJoint.create()
					.setReducerCount(1)
					.addMapper(this)
					.addReducer(this);
	}
	
	@Override
	public RecordSet apply(RecordSet input) {
		return new RSet(this, input);
	}
	
	private static class RSet extends SingleInputRecordSet<PickTopK> {
		private MinMaxPriorityQueue<RecordNode> m_queue = null;
		
		private RSet(PickTopK pick, RecordSet input) {
			super(pick, input);
			
			m_queue = fillPriorityQueue(pick.m_sortKeyCols, pick.m_topK);
		}

		@Override
		public Record nextCopy() {
			checkNotClosed();
			
			if ( !m_queue.isEmpty() ) {
				return m_queue.poll().m_record;
			}
			else {
				return null;
			}
		}
		
		private MinMaxPriorityQueue<RecordNode> fillPriorityQueue(MultiColumnKey keyCols, int length) {
			MinMaxPriorityQueue<RecordNode> queue = MinMaxPriorityQueue.maximumSize(length).create();
			
			m_input.forEach(rec -> queue.add(new RecordNode(keyCols, rec.duplicate())));
			return queue;
		}
	}
	
	@Override
	public String toString() {
		return String.format("pick_topk[k=%d, {%s}]", m_topK, m_sortKeyCols);
	}

	public static PickTopK fromProto(PickTopKProto proto) {
		return PickTopK.builder()
						.sortKeyColumns(proto.getSortKeyColumns())
						.topK(proto.getTopK())
						.build();
	}

	@Override
	public PickTopKProto toProto() {
		return PickTopKProto.newBuilder()
							.setSortKeyColumns(m_sortKeyCols.toString())
							.setTopK(m_topK)
							.build();
	}
	
	private static class RecordNode implements Comparable<RecordNode> {
		private final RecordKey m_key;
		final Record m_record;
		
		RecordNode(MultiColumnKey keyCols, Record record) {
			m_key = RecordKey.from(keyCols, record);
			m_record = record;
		}

		@Override
		public int compareTo(RecordNode o) {
			return m_key.compareTo(o.m_key);
		}
	}
	
	public static Builder builder() {
		return new Builder();
	}
	public static class Builder {
		private MultiColumnKey m_sortKeyCols;
		private int m_k;
		
		public PickTopK build() {
			return new PickTopK(m_sortKeyCols, m_k);
		}
		
		public Builder sortKeyColumns(String keySpec) {
			m_sortKeyCols = MultiColumnKey.fromString(keySpec);
			return this;
		}
		
		public Builder sortKeyColumns(MultiColumnKey key) {
			m_sortKeyCols = key;
			return this;
		}
		
		public Builder topK(int k) {
			m_k = k;
			return this;
		}
	}
}
