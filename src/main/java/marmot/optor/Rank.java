package marmot.optor;

import java.util.Iterator;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordComparator;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.io.MultiColumnKey;
import marmot.io.RecordKey;
import marmot.mapreduce.MarmotMapOutputKeyColumns;
import marmot.optor.rset.SingleInputRecordSet;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.proto.optor.RankProto;
import marmot.support.PBSerializable;
import marmot.type.DataType;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Rank extends AbstractRecordSetFunction implements PBSerializable<RankProto> {
	public static final String DEF_RANK_COL_NAME = "rank";
	
	public enum Type { MAP_REDUCE, LOCAL };
	
	private final MultiColumnKey m_sortKeyCols;
	private final FOption<String> m_rankOutputColumn;
	private final Type m_type;
	
	private Rank(MultiColumnKey orderKeyCols, FOption<String> rankOutputColumn, Type type) {
		m_sortKeyCols = orderKeyCols;
		m_rankOutputColumn = rankOutputColumn;
		m_type = type;
	}
	
	public MultiColumnKey getSortKeyColumns() {
		return m_sortKeyCols;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		RecordSchema outSchema = inputSchema.toBuilder()
										.addColumn(m_rankOutputColumn.getOrElse(DEF_RANK_COL_NAME),
													DataType.LONG)
										.build();
		setInitialized(marmot, inputSchema, outSchema);
	}
	
	@Override
	public boolean isStreamable() {
		return false;
	}

	@Override
	public MapReduceJoint getMapReduceJoint(RecordSchema inputSchema) {
		if ( m_marmot == null ) {
			throw new IllegalArgumentException("not initialized: optor=" + this);
		}
		
		Rank rank = new Rank(m_sortKeyCols, m_rankOutputColumn, Type.LOCAL);
		rank.initialize(m_marmot, inputSchema);
		
		return MapReduceJoint.create()
					.setMapOutputKey(MarmotMapOutputKeyColumns.fromSortKey(m_sortKeyCols))
					.setReducerCount(1)
					.addReducer(rank);
	}

	@Override
	public RecordSet apply(RecordSet input) {
		return new RSet(this, input);
	}
	
	@Override
	public String toString() {
		String outCol = m_rankOutputColumn.map(s -> "->" + s)
											.getOrElse("");
		return String.format("rank[{%s}%s]", m_sortKeyCols, outCol);
	}

	public static Rank fromProto(RankProto proto) {
		Rank.Builder builder = Rank.builder()
									.sortKeyColumns(proto.getSortKeyColumns());
		switch ( proto.getOptionalRankColumnCase() ) {
			case RANK_COLUMN:
				builder = builder.rankColumn(proto.getRankColumn());
				break;
			default:
				
		}
		return builder.build();
	}

	@Override
	public RankProto toProto() {
		RankProto.Builder builder = RankProto.newBuilder()
											.setSortKeyColumns(m_sortKeyCols.toString());
		m_rankOutputColumn.ifPresent(builder::setRankColumn);
		return builder.build();
	}
	
	private static class RSet extends SingleInputRecordSet<Rank> {
		private final int m_outColIndex;
		private Iterator<Record> m_sorteds;
		
		private RecordKey m_lastKey = null;
		private long m_recordCount;
		private long m_rank;
		
		private RSet(Rank rank, RecordSet input) {
			super(rank, input);
			
			m_recordCount = 0;
			m_outColIndex = input.getRecordSchema().getColumnCount();
		}

		@Override
		public boolean next(Record record) {
			checkNotClosed();
			
			if ( m_sorteds == null ) {
				RecordComparator cmp = new RecordComparator(m_optor.m_sortKeyCols);
				m_sorteds = m_input.fstream().sort(cmp).iterator();
			}
			
			if ( !m_sorteds.hasNext() ) {
				return false;
			}
			
			record.set(m_sorteds.next());
			RecordKey key = RecordKey.from(m_optor.m_sortKeyCols, record);
			if ( m_lastKey == null || !m_lastKey.equals(key) ) {
				m_lastKey = key;
				m_rank = m_recordCount;
			}
			record.set(m_outColIndex, m_rank);
			++m_recordCount;
			
			return true;
		}
	}
	
	public static Builder builder() {
		return new Builder();
	}
	public static class Builder {
		private MultiColumnKey m_orderKeyCols;
		private FOption<String> m_rankColumnName = FOption.empty();
		private Type m_type = Type.MAP_REDUCE;
		
		/**
		 * {@link Rank} 객체 생성한다.
		 * 
		 * @return 생성될 Rank 연산자.
		 */
		public Rank build() {
			return new Rank(m_orderKeyCols, m_rankColumnName, m_type);
		}
		
		public Builder sortKeyColumns(String keyExpr) {
			m_orderKeyCols = MultiColumnKey.fromString(keyExpr);
			return this;
		}
		
		public Builder sortKeyCols(MultiColumnKey key) {
			m_orderKeyCols = key;
			return this;
		}
		
		public Builder rankColumn(String colName) {
			m_rankColumnName = FOption.ofNullable(colName);
			return this;
		}
		
		public Builder rankColumn(FOption<String> colName) {
			m_rankColumnName = colName;
			return this;
		}
		
		public Builder type(Type type) {
			m_type = type;
			return this;
		}
	}
}