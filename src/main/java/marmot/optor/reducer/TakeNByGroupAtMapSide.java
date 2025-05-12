package marmot.optor.reducer;

import java.util.Map;

import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;

import utils.stream.FStream;
import utils.stream.KeyValueFStream;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.io.MultiColumnKey;
import marmot.io.RecordKey;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.proto.optor.TakeNByGroupAtMapSideProto;
import marmot.support.PBSerializable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TakeNByGroupAtMapSide extends AbstractRecordSetFunction
									implements PBSerializable<TakeNByGroupAtMapSideProto> {
	private final MultiColumnKey m_grpKeys;
	private final MultiColumnKey m_tagKeys;
	private final MultiColumnKey m_orderKeys;
	private final int m_takeCount;
	
	private KeyedRecordComparator m_cmptor;
	
	private class Node implements Comparable<Node> {
		private final Record m_record;
		
		Node(Record rec) {
			m_record = rec;
		}

		@Override
		public int compareTo(Node o) {
			return m_cmptor.compare(m_record, o.m_record);
		}
		
		@Override
		public String toString() {
			return m_record.toString();
		}
	}
	
	public TakeNByGroupAtMapSide(MultiColumnKey keys, MultiColumnKey tagKeys,
								MultiColumnKey orderKeys, int takeCount) {
		m_grpKeys = keys;
		m_tagKeys = tagKeys;
		m_orderKeys = orderKeys;
		m_takeCount = takeCount;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		m_marmot = marmot;

		m_cmptor = new KeyedRecordComparator(m_orderKeys, inputSchema);
		setInitialized(marmot, inputSchema, inputSchema);
	}

	@Override
	public RecordSet apply(RecordSet input) {
		checkInitialized();

		Map<RecordKey,MinMaxPriorityQueue<Node>> groups = Maps.newHashMap();
		for ( Record rec = input.nextCopy(); rec != null; rec = input.nextCopy() ) {
			RecordKey key = RecordKey.from(m_grpKeys, rec);
			MinMaxPriorityQueue<Node> queue = groups.get(key);
			if ( queue == null ) {
				queue = MinMaxPriorityQueue.maximumSize(m_takeCount).create();
				groups.put(key, queue);
			}
			
			queue.add(new Node(rec));
		}
		
		return RecordSet.from(KeyValueFStream.from(groups)
											.flatMap(kv -> FStream.from(kv.value()))
											.map(n -> n.m_record));
	}


	public static TakeNByGroupAtMapSide fromProto(TakeNByGroupAtMapSideProto proto) {
		MultiColumnKey grpKeys = MultiColumnKey.fromString(proto.getKeyColumns());
		MultiColumnKey tagKeys = MultiColumnKey.fromString(proto.getTagColumns());
		MultiColumnKey orderKeys = MultiColumnKey.fromString(proto.getOrderColumns());
		int takeCount = proto.getTakeCount();
		
		return new TakeNByGroupAtMapSide(grpKeys, tagKeys, orderKeys, takeCount);
	}
	
	@Override
	public TakeNByGroupAtMapSideProto toProto() {
		return TakeNByGroupAtMapSideProto.newBuilder()
										.setKeyColumns(m_grpKeys.toString())
										.setTagColumns(m_tagKeys.toString())
										.setOrderColumns(m_orderKeys.toString())
										.setTakeCount(m_takeCount)
										.build();
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder("topk_by_group_at_mapside: ");
		
		builder.append("keys={").append(m_grpKeys).append('}');
		if ( m_tagKeys.length() > 0 ) {
			builder.append(",tags={").append(m_tagKeys).append('}');
		}
		if ( m_orderKeys.length() > 0 ) {
			builder.append(String.format(", order_by={%s}", m_orderKeys));
		}
		builder.append(String.format("count=%d]", m_takeCount));
		
		return String.format(builder.toString());
	}
}
