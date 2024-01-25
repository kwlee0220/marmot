package marmot.optor.join;

import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.io.RecordWritable;
import marmot.optor.JoinType;
import marmot.optor.RecordSetOperatorException;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.optor.support.JoinUtils;
import marmot.optor.support.KeyedRecordSet;
import marmot.optor.support.colexpr.ColumnSelector;
import marmot.proto.optor.JoinPartitionPairProto;
import marmot.rset.PushBackableRecordSet;
import marmot.support.DefaultRecord;
import marmot.support.PBSerializable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JoinPartitionPair extends AbstractRecordSetFunction
								implements PBSerializable<JoinPartitionPairProto>  {
	public static final int TAG_IDX_DATASET_IDX = 0;
	public static final int TAG_IDX_CACHEABLE = 1;
	public static final int TAG_IDX_DECODED = 2;

	private final String m_leftPrefix;
	private final RecordSchema m_leftSchema;
	private final String m_rightPrefix;
	private final RecordSchema m_rightSchema;
	private final JoinType m_joinType;
	private final String m_outputColumns;

	private int m_cachingDsIdx = -1;
	private ColumnSelector m_selector;
	
	public JoinPartitionPair(String leftPrefix, RecordSchema leftSchema,
							String rightPrefix, RecordSchema rightSchema,
							String outputColumns, JoinType joinType) {
		m_leftPrefix = leftPrefix;
		m_leftSchema = leftSchema;
		m_rightPrefix = rightPrefix;
		m_rightSchema = rightSchema;
		m_outputColumns = outputColumns;
		m_joinType = joinType;
	}
	
	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		m_marmot = marmot;
		
		try {
			m_selector = JoinUtils.createJoinColumnSelector(m_leftSchema, m_rightSchema, m_outputColumns);
			
			RecordSchema outSchema = (m_joinType == JoinType.SEMI_JOIN)
										? inputSchema : m_selector.getRecordSchema();
			setInitialized(marmot, inputSchema, outSchema);
		}
		catch ( Exception e ) {
 			throw new RecordSetOperatorException(String.format("op=%s, cause=%s", this, e));
		}
	}

	@Override
	public RecordSet apply(RecordSet input) {
		checkInitialized();
		Preconditions.checkArgument(input instanceof KeyedRecordSet);
		
		KeyedRecordSet group = (KeyedRecordSet)input;
		
		PushBackableRecordSet pbrset = group.asPushBackable();
		List<Record> cache = loadCachingPartition(pbrset);
		RecordSet nonCached = new DecodedRecordSet(pbrset,
											(m_cachingDsIdx == 1) ? m_leftSchema : m_rightSchema);
		pbrset = nonCached.asPushBackable();

		switch ( m_joinType ) {
			case INNER_JOIN:
				return ( cache.size() > 0 && pbrset.hasNext() )
						? new PartitionInnerJoin(pbrset, cache, m_cachingDsIdx, m_selector)
						: RecordSet.empty(getRecordSchema());
			case LEFT_OUTER_JOIN:
			case SEMI_JOIN:
				return leftOuterJoin(cache, pbrset);
			case RIGHT_OUTER_JOIN:
				return rightOuterJoin(cache, pbrset);
			case FULL_OUTER_JOIN:
				return fullOuterJoin(cache, pbrset);
			default:
				throw new AssertionError("unknown join type: type=" + m_joinType);
		}
	}

	public static JoinPartitionPair fromProto(JoinPartitionPairProto proto) {
		return new JoinPartitionPair(proto.getLeftPrefix(),
									RecordSchema.fromProto(proto.getLeftSchema()),
									proto.getRightPrefix(),
									RecordSchema.fromProto(proto.getRightSchema()),
									proto.getOutputColumnsExpr(),
									JoinType.fromProto(proto.getJoinType()));
	}

	@Override
	public JoinPartitionPairProto toProto() {
		return JoinPartitionPairProto.newBuilder()
									.setLeftPrefix(m_leftPrefix)
									.setLeftSchema(m_leftSchema.toProto())
									.setRightPrefix(m_rightPrefix)
									.setRightSchema(m_rightSchema.toProto())
									.setJoinType(m_joinType.toProto())
									.setOutputColumnsExpr(m_outputColumns)
									.build();
	}
	
	private RecordSet leftOuterJoin(List<Record> cache, PushBackableRecordSet rset) {
		if ( cache.size() == 0 ) {
			if ( m_cachingDsIdx == 1 ) {
				// 오른쪽 dataset이 empty인 경우.
				return new SingleSidePartitionJoin(m_leftSchema, m_rightSchema, rset, 0, m_selector);
			}
			else {
				// 왼쪽 dataset이 empty인 경우.
				return RecordSet.empty(getRecordSchema());
			}
		}
		if ( !rset.hasNext() ) {
			if ( m_cachingDsIdx == 1 ) {
				// 왼쪽 dataset이 empty인 경우.
				return RecordSet.empty(getRecordSchema());
			}
			else {
				// 오른쪽 dataset이 empty인 경우.
				return new SingleSidePartitionJoin(m_leftSchema, m_rightSchema,
													RecordSet.from(cache), 0, m_selector);
			}
		}
		
		return new PartitionInnerJoin(rset, cache, m_cachingDsIdx, m_selector);
	}
	
	private RecordSet rightOuterJoin(List<Record> cache, PushBackableRecordSet rset) {
		if ( cache.size() == 0 ) {
			if ( m_cachingDsIdx == 1 ) {
				// 오른쪽 dataset이 empty인 경우.
				return RecordSet.empty(getRecordSchema());
			}
			else {
				// 왼쪽 dataset이 empty인 경우.
				return new SingleSidePartitionJoin(m_leftSchema, m_rightSchema, rset, 1, m_selector);
			}
		}
		if ( !rset.hasNext() ) {
			if ( m_cachingDsIdx == 1 ) {
				// 왼쪽 dataset이 empty인 경우.
				return new SingleSidePartitionJoin(m_leftSchema, m_rightSchema,
													RecordSet.from(cache), 1, m_selector);
			}
			else {
				// 오른쪽 dataset이 empty인 경우.
				return RecordSet.empty(getRecordSchema());
			}
		}
		
		return new PartitionInnerJoin(rset, cache, m_cachingDsIdx, m_selector);
	}
	
	private RecordSet fullOuterJoin(List<Record> cache, PushBackableRecordSet rset) {
		if ( cache.size() == 0 ) {
			if ( m_cachingDsIdx == 1 ) {
				// 오른쪽 dataset이 empty인 경우.
				return new SingleSidePartitionJoin(m_leftSchema, m_rightSchema, rset, 0, m_selector);
			}
			else {
				// 왼쪽 dataset이 empty인 경우.
				return new SingleSidePartitionJoin(m_leftSchema, m_rightSchema, rset, 1, m_selector);
			}
		}
		if ( !rset.hasNext() ) {
			if ( m_cachingDsIdx == 1 ) {
				// 왼쪽 dataset이 empty인 경우.
				return new SingleSidePartitionJoin(m_leftSchema, m_rightSchema,
													RecordSet.from(cache), 1, m_selector);
			}
			else {
				// 오른쪽 dataset이 empty인 경우.
				return new SingleSidePartitionJoin(m_leftSchema, m_rightSchema,
													RecordSet.from(cache), 0, m_selector);
			}
		}
		
		return new PartitionInnerJoin(rset, cache, m_cachingDsIdx, m_selector);
	}
	
	protected List<Record> loadCachingPartition(PushBackableRecordSet pbrset) {
		Record peeked = pbrset.nextCopy();

		int dsIdx = peeked.getByte(TAG_IDX_DATASET_IDX);
		if ( m_cachingDsIdx == -1 ) {
			boolean cacheable = peeked.getByte(TAG_IDX_CACHEABLE) == 1;
			m_cachingDsIdx = (cacheable) ? dsIdx : ((dsIdx == 0) ? 1 : 0);
		}
		pbrset.pushBack(peeked);
		
		List<Record> collecteds = Lists.newArrayList();
		
		// caching 대상이되는 dataset의 데이터가 없는 경우.
		if ( peeked.getByte(TAG_IDX_CACHEABLE) == 0 ) {
			return collecteds;
		}
		
		RecordSchema cachingDsSchema = (m_cachingDsIdx == 0)
										? m_leftSchema : m_rightSchema;
		Record encoded = DefaultRecord.of(pbrset.getRecordSchema());
		while ( pbrset.next(encoded) ) {
			// dataset index를 읽어서 caching해야 할 데이터를 모두 읽었으면 caching을 종료시킨다.
			dsIdx = encoded.getByte(TAG_IDX_DATASET_IDX);
			if ( dsIdx != m_cachingDsIdx ) {
				pbrset.pushBack(encoded);
				return collecteds;
			}
			
			byte[] bytes = (byte[])encoded.get(MapReduceHashJoin.TAG_IDX_DECODED);
			Record decoded = RecordWritable.fromBytes(bytes, cachingDsSchema);
			collecteds.add(decoded);
		}
		
		return collecteds;
	}

}
