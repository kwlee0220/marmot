package marmot.optor.join;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.io.RecordWritable;
import marmot.optor.support.KeyedRecordSetFactory;
import marmot.optor.support.colexpr.ColumnSelector;
import marmot.rset.AbstractRecordSet;
import marmot.rset.PushBackableRecordSet;
import marmot.support.DefaultRecord;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
final class JoinedRecordSet extends AbstractRecordSet {
	private final JoinPartitions m_join;
	private final KeyedRecordSetFactory m_groups;
	private final RecordSchema m_leftSchema;
	private final RecordSchema m_rightSchema;
	private final ColumnSelector m_selector;
	private final Map<String,Record> m_binding = Maps.newHashMap();

	private List<Record> m_cache;
	private PushBackableRecordSet m_leftRSet;
	private PushBackableRecordSet m_rightRSet;
	private final Record m_leftRecord;
	private final Record m_rightRecord;
	private int m_cachingDsIdx = -1;
	private boolean m_loadNextPair = true;
	
	JoinedRecordSet(JoinPartitions join, KeyedRecordSetFactory groups,
					RecordSchema leftSchema, RecordSchema rightSchema,
					ColumnSelector selector) {
		m_join = join;
		m_groups = groups;
		m_leftSchema = leftSchema;
		m_rightSchema = rightSchema;
		m_selector = selector;

		m_leftRecord = DefaultRecord.of(leftSchema);
		m_rightRecord = DefaultRecord.of(rightSchema);
	}
	
	@Override
	protected void closeInGuard() {
		m_groups.closeQuietly();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_join.getRecordSchema();
	}
	
	public boolean isLeftPartitionCached() {
		return m_cachingDsIdx == 0;
	}

	@Override
	public boolean next(Record output) {
		while ( true ) {
			if ( m_loadNextPair ) {
				// 다음번 조인에 참여할 왼쪽, 오른쪽 partition을 적재한다.
				if ( !loadNextPartitionPair() ) {
					return false;
				}

				setupFirstOuterLoop();
				m_loadNextPair = false;
			}
			
			if ( nextMatch(output) ) {
				return true;
			}
			m_loadNextPair = true;
		}
	}
	
	@Override
	public String toString() {
		return m_join.toString();
	}
	
	protected boolean loadNextPartitionPair() {
		while ( true ) {
			PushBackableRecordSet group = m_groups.nextKeyedRecordSet()
												.map(RecordSet::asPushBackable)
												.getOrNull();
			if ( group == null ) {
				return false;
			}
			
			m_cache = loadCachingPartition(group);
			if ( m_cachingDsIdx == 0 ) {	// chaching left
				m_leftRSet = RecordSet.from(m_leftSchema, m_cache).asPushBackable();
				m_rightRSet = new DecodedRecordSet(group, m_rightSchema).asPushBackable();
			}
			else {					// caching right
				m_leftRSet = new DecodedRecordSet(group, m_leftSchema).asPushBackable();
				m_rightRSet = RecordSet.from(m_rightSchema, m_cache).asPushBackable();
			}
			
			if ( !m_leftRSet.hasNext() ) {
				m_leftRSet = null;
			}
			if ( !m_rightRSet.hasNext() ) {
				m_rightRSet = null;
			}
			m_binding.put(m_join.getLeftNamespace(), null);
			m_binding.put(m_join.getRightNamespace(), null);
			
			switch ( m_join.getJoinType() ) {
				case INNER_JOIN:
					if ( m_leftRSet != null && m_rightRSet != null ) {
						return true;
					}
					break;
				case LEFT_OUTER_JOIN:
					if ( m_leftRSet != null ) {
						return true;
					}
					break;
				case RIGHT_OUTER_JOIN:
					if ( m_rightRSet != null ) {
						return true;
					}
					break;
				case FULL_OUTER_JOIN:
					if ( m_leftRSet != null || m_rightRSet != null ) {
						return true;
					}
					break;
				default:
					throw new AssertionError();
			}
		}
	}
	
	protected boolean nextMatch(Record output) {
		if ( m_leftRSet != null && m_rightRSet != null ) {
			return nextInnerJoinedRecord(output);
		}
		if ( m_leftRSet != null ) {
			return nextLeftOuterJoinedRecord(output);
		}
		if ( m_rightRSet != null ) {
			return nextRightOuterJoinedRecord(output);
		}
		
		throw new AssertionError();
	}
	
	protected boolean nextInnerJoinedRecord(Record output) {
		if ( isLeftPartitionCached() ) {
			while ( true ) {
				if ( m_leftRSet.next(m_leftRecord) ) {
					m_binding.put(m_join.getLeftNamespace(), m_leftRecord);
					m_selector.select(m_binding, output);
					
					return true;
				}
				
				Record record = m_rightRSet.nextCopy();
				if ( record != null ) {
					m_binding.put(m_join.getRightNamespace(), record);
					rewindCache();
				}
				else {
					return false;
				}
			}
		}
		else {
			while ( true ) {
				if ( m_rightRSet.next(m_rightRecord) ) {
					m_binding.put(m_join.getRightNamespace(), m_rightRecord);
					m_selector.select(m_binding, output);
					
					return true;
				}
				
				Record record = m_leftRSet.nextCopy();
				if ( record != null ) {
					m_binding.put(m_join.getLeftNamespace(), record);
					rewindCache();
				}
				else {
					return false;
				}
			}
		}
	}
	
	protected boolean nextRightOuterJoinedRecord(Record output) {
		Record record = m_rightRSet.nextCopy();
		if ( record != null ) {
			m_binding.put(m_join.getRightNamespace(), record);
			m_selector.select(m_binding, output);
			
			return true;
		}
		else {
			return false;
		}
	}
	
	protected boolean nextLeftOuterJoinedRecord(Record output) {
		Record next = m_leftRSet.nextCopy();
		if ( next != null ) {
			m_binding.put(m_join.getLeftNamespace(), next);
			m_selector.select(m_binding, output);
		}
		
		return next != null;
	}
	
	protected void rewindCache() {
		if ( isLeftPartitionCached() ) {
			m_leftRSet = RecordSet.from(m_leftSchema, m_cache).asPushBackable();
		}
		else {
			m_rightRSet = RecordSet.from(m_rightSchema, m_cache).asPushBackable();
		}
	}
	
	protected void setupFirstOuterLoop() {		
		if ( isLeftPartitionCached() ) {
			// right가 outer-loop 됨
			if ( m_rightRSet != null && m_leftRSet != null ) {
				m_binding.put(m_join.getRightNamespace(), m_rightRSet.nextCopy());
			}
			else if ( m_rightRSet != null ) {
				m_binding.put(m_join.getLeftNamespace(), null);
			}
			else {
				m_binding.put(m_join.getRightNamespace(), null);
			}
		}
		else {
			// left가 outer-loop 됨
			if ( m_leftRSet != null && m_rightRSet != null ) {
				m_binding.put(m_join.getLeftNamespace(), m_leftRSet.nextCopy());
			}
			else if ( m_leftRSet != null ) {
				m_binding.put(m_join.getRightNamespace(), null);
			}
			else {
				m_binding.put(m_join.getLeftNamespace(), null);
			}
		}
	}
	
	protected List<Record> loadCachingPartition(PushBackableRecordSet group) {
		List<Record> collecteds = Lists.newArrayList();
		Record encoded = DefaultRecord.of(m_groups.getRecordSchema());
		
		if ( m_cachingDsIdx == -1 ) {
			Record peeked = group.peekCopy();
			
			int dsIdx = peeked.getByte(MapReduceHashJoin.TAG_IDX_DATASET_IDX);
			boolean cacheable = peeked.getByte(MapReduceHashJoin.TAG_IDX_CACHEABLE) == 1;
			m_cachingDsIdx = (cacheable) ? dsIdx : ((dsIdx == 0) ? 1 : 0);
		}
		
		RecordSchema cachingDsSchema = (m_cachingDsIdx ==0) ? m_leftSchema : m_rightSchema;
		while ( group.next(encoded) ) {
			boolean cacheable = encoded.getByte(MapReduceHashJoin.TAG_IDX_CACHEABLE) == 1;
			if ( !cacheable ) {
				group.pushBack(encoded);
				break;
			}

			byte[] bytes = (byte[])encoded.get(MapReduceHashJoin.TAG_IDX_DECODED);
			Record decoded = RecordWritable.fromBytes(bytes, cachingDsSchema);
			collecteds.add(decoded);
		}
		
		return collecteds;
	}
}
