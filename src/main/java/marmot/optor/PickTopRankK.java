package marmot.optor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.io.MultiColumnKey;
import marmot.io.RecordKey;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.rset.AbstractRecordSet;
import marmot.support.DefaultRecord;
import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PickTopRankK extends AbstractRecordSetFunction {
	private static final String DEFAULT_RANK_COLUMN = "rank";
	
	private final MultiColumnKey m_sortKeyCols;
	private final int m_topK;
	private final String m_rankCol;
	
	private PickTopRankK(MultiColumnKey sortKeyCols, int topK, String rankColumnName) {
		m_sortKeyCols = sortKeyCols;
		m_topK = topK;
		m_rankCol = rankColumnName;
	}
	
	@Override
	public boolean isStreamable() {
		return false;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		m_marmot = marmot;
		RecordSchema outSchema = inputSchema.toBuilder()
											.addOrReplaceColumn(m_rankCol, DataType.INT)
											.build();
		
		setInitialized(marmot, inputSchema, outSchema);
	}

	@Override
	public RecordSet apply(RecordSet input) {
		if ( m_marmot == null ) {
			throw new IllegalArgumentException("not initialized: optor=" + this);
		}
		
		return new TopKRankPicked(m_marmot, this, input);
	}

	@Override
	public MapReduceJoint getMapReduceJoint(RecordSchema inputSchema) {
		return MapReduceJoint.create().setReducerCount(1);
	}
	
	@Override
	public String toString() {
		return String.format("topk[{%s}->%s]", m_sortKeyCols, m_rankCol);
	}
	
	private static class TopKRankPicked extends AbstractRecordSet {
		private final PickTopRankK m_optor;
		private final RecordSet m_input;
		private final MultiColumnKey m_sortKeyCols;
		private final int m_topK;
		private final int m_rankColIdx;
		
		private Iterator<Map.Entry<RecordKey,List<Record>>> m_iter = null;
		private Iterator<Record> m_tieIter = null;
		private int m_lastRank;
		private int m_count;
		
		public TopKRankPicked(MarmotCore marmot, PickTopRankK pick, RecordSet input) {
			m_optor = pick;
			m_input = input;
			
			m_sortKeyCols = pick.m_sortKeyCols;
			m_topK = pick.m_topK;
			m_rankColIdx = getRecordSchema().getColumn(pick.m_rankCol).ordinal();
		}
		
		@Override
		protected void closeInGuard() {
			m_iter = null;
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_optor.getRecordSchema();
		}
	
		@Override
		public boolean next(Record record) {
			checkNotClosed();
			
			if ( m_iter == null ) {
				m_iter = loadInputRecordSet().iterator();
				if ( m_iter.hasNext() ) {
					m_tieIter = m_iter.next().getValue().iterator();
				}
				else {
					m_tieIter = new ArrayList<Record>().iterator();
					return false;
				}
				m_lastRank = 1;
				m_count = 1;
			}
			
			while ( true ) {
				if ( m_tieIter.hasNext() ) {
					Record ranked = m_tieIter.next();
					record.set(ranked);
					record.set(m_rankColIdx, m_lastRank);
					++m_count;
					
					return true;
				}
				else if ( m_iter.hasNext() ) {
					Map.Entry<RecordKey, List<Record>> entry = m_iter.next();
					m_tieIter = entry.getValue().iterator();
					m_lastRank = m_count;
				}
				else {
					return false;
				}
			}
		}
		
		private Set<Map.Entry<RecordKey,List<Record>>> loadInputRecordSet() {
			final TreeMap<RecordKey,List<Record>> chart = Maps.newTreeMap();
			final Record record = DefaultRecord.of(m_input.getRecordSchema());
			int m_chartSize = 0;
			
			while ( m_input.next(record) ) {
				RecordKey key = RecordKey.from(m_sortKeyCols, record);
				
				if ( m_chartSize >= m_topK && chart.lastKey().compareTo(key) < 0 ) {
					continue;
				}
				
				List<Record> tieList = chart.get(key);
				if ( tieList == null ) {
					tieList = Lists.newArrayList();
					chart.put(key, tieList);
				}
				tieList.add(record.duplicate());
				
				if ( ++m_chartSize > m_topK ) {
					tieList = chart.get(chart.lastKey());
					if ( (m_chartSize - tieList.size()) >= m_topK ) {
						m_chartSize -= tieList.size();
						chart.remove(chart.lastKey());
					}
				}
			}
			
			return chart.entrySet();
		}
	}

//	public static PickTopRankK fromXml(FluentElement rankElm) {
//		Builder builder = builder();
//		
//		builder.sortKeyColumns(MultiColumnKey.fromXml(rankElm.firstChild("sort_key_columns")));
//		builder.topK(rankElm.firstChild("top_k").textInt().get());
//		builder.rankColumn(rankElm.firstChild("rank_column").text().get());
//		
//		return builder.build();
//	}
//	
//	@Override
//	public void toXml(FluentElement pickElm) {
//		pickElm.appendChild("sort_key_columns", m_sortKeyCols);
//		pickElm.appendChild("top_k").text(m_topK);
//		pickElm.appendChild("rank_column").text(m_rankCol);
//	}
	
	public static Builder builder() {
		return new Builder();
	}
	public static class Builder {
		private MultiColumnKey m_sortKeyCols;
		private int m_topK;
		private String m_rankColumn = DEFAULT_RANK_COLUMN;
		
		public PickTopRankK build() {
			return new PickTopRankK(m_sortKeyCols, m_topK, m_rankColumn);
		}
		
		public Builder sortKeyColumns(String keySpec) {
			m_sortKeyCols = MultiColumnKey.fromString(keySpec);
			return this;
		}
		
		public Builder sortKeyColumns(MultiColumnKey key) {
			m_sortKeyCols = key;
			return this;
		}
		
		public Builder topK(int topK) {
			Preconditions.checkArgument(topK > 0);
			
			m_topK = topK;
			return this;
		}
		
		public Builder rankColumn(String name) {
			m_rankColumn = name;
			return this;
		}
	}
}