package marmot.optor.rset;

import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;

import com.google.common.base.Preconditions;

import utils.StopWatch;
import utils.Utilities;
import utils.func.FOption;
import utils.stream.KeyValueFStream;
import utils.stream.KeyedGroups;

import marmot.Record;
import marmot.RecordComparator;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.io.MultiColumnKey;
import marmot.io.RecordKey;
import marmot.optor.support.KeyedRecordSet;
import marmot.optor.support.KeyedRecordSetFactory;
import marmot.rset.AbstractRecordSet;
import marmot.support.ProgressReportable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class InMemoryKeyedRecordSetFactory implements KeyedRecordSetFactory {
	private final RecordSet m_src;
	private final MultiColumnKey m_keyCols;
	private final MultiColumnKey m_tagCols;;
	private final MultiColumnKey m_orderCols;
	private final MultiColumnKey m_groupCols;
	private final RecordSchema m_groupKeySchema;
	private final RecordSchema m_outSchema;
	private final boolean m_sortByKey;
	private KeyValueFStream<RecordKey,List<Record>> m_groups;
	
	private int m_groupCount;
	
	public InMemoryKeyedRecordSetFactory(RecordSet src, MultiColumnKey keyCols,
										MultiColumnKey tagCols, MultiColumnKey orderCols,
										boolean sortByKey) {
		Utilities.checkNotNullArgument(keyCols, "Key columns should not be null");
		Utilities.checkNotNullArgument(tagCols, "Tag key columns should not be null");
		
		m_src = src;
		m_keyCols = keyCols;
		m_tagCols = tagCols;
		m_orderCols = orderCols;
		m_groupCols = MultiColumnKey.concat(m_keyCols, tagCols);
		m_sortByKey = sortByKey;
		
		RecordSchema inputSchema = src.getRecordSchema();
		m_groupKeySchema = inputSchema.project(m_groupCols.getColumnNames());
		MultiColumnKey valueCols = m_groupCols.complement(inputSchema);
		m_outSchema = inputSchema.project(valueCols.getColumnNames());
	}
	
	public InMemoryKeyedRecordSetFactory(RecordSet src, MultiColumnKey keyCols, boolean sortByKey) {
		this(src, keyCols, MultiColumnKey.EMPTY, MultiColumnKey.EMPTY, sortByKey);
	}
	
	@Override
	public RecordSchema getRecordSchema() {
		return m_outSchema;
	}

	@Override
	public MultiColumnKey getKeyColumns() {
		return m_groupCols;
	}

	@Override
	public RecordSchema getKeySchema() {
		return m_groupKeySchema;
	}
	
	@Override
	public FOption<KeyedRecordSet> nextKeyedRecordSet() {
		if ( m_groups == null ) {	// is this the first call?
			m_groups = buildGroups();
		}
		
		return m_groups.next()
						.map(kv -> new InMemoryKeyedRecordSet(this, kv.key(), kv.value()));
	}

	@Override
	public void reportProgress(Logger logger, StopWatch elapsed) {
		if ( m_src instanceof ProgressReportable ) {
			((ProgressReportable)m_src).reportProgress(logger, elapsed);
		}
		
		logger.info("report: {}, elapsed={}s", toString(), elapsed.getElapsedSecondString());
	}
	
	@Override
	public String toString() {
		String tagStr = ( m_tagCols.length() > 0 )
						? String.format(",tags={%s}", m_tagCols) : "";
		String orderStr = ( m_orderCols.length() > 0 )
						? String.format(",order_keys={%s}", m_orderCols) : "";
		return String.format("grouped_rset_factory[keys={%s}%s%s]: ngroups=%d",
								m_keyCols, tagStr, orderStr, m_groupCount);
	}
	
	public void close() throws Exception {
		m_src.close();
	}
	
	private KeyValueFStream<RecordKey,List<Record>> buildGroups() {
		try  {
			KeyedGroups<RecordKey,Record> keyeds = m_src.fstream()
														.tagKey(r -> RecordKey.from(m_keyCols, r))
														.groupByKey();
			m_groupCount = keyeds.groupCount();
			
			KeyValueFStream<RecordKey,List<Record>> groups = keyeds.fstream();
			if ( m_orderCols.length() > 0 ) {
				groups = groups.mapValue(this::sortValues);
			}
			
			if ( m_sortByKey ) {
				return groups.sortByKey();
			}
			else {
				return groups;
			}
		}
		finally {
			m_src.closeQuietly();
		}
	}
	
	private List<Record> sortValues(RecordKey key, List<Record> records) {
		records.sort(new RecordComparator(m_orderCols));
		return records;
	}

	private static class InMemoryKeyedRecordSet extends AbstractRecordSet
												implements KeyedRecordSet {
		private final InMemoryKeyedRecordSetFactory m_fact;
		private final RecordKey m_groupKey;
		private Iterator<Record> m_iter;
		private long m_count = 0;
		
		InMemoryKeyedRecordSet(InMemoryKeyedRecordSetFactory fact, RecordKey key, List<Record> records) {
			Utilities.checkNotNullArgument(key, "Group key");
			Utilities.checkNotNullArgument(records, "Group RecordSet");
			Preconditions.checkArgument(records.size() > 0, "Group RecordSet is empty");
			
			m_fact = fact;
			m_groupKey = RecordKey.from(fact.m_groupCols, records.get(0));
			m_iter = records.iterator();
		}
		
		@Override protected void closeInGuard() { }

		@Override
		public RecordKey getKey() {
			return m_groupKey;
		}

		@Override
		public RecordSchema getKeySchema() {
			return m_fact.m_groupKeySchema;
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_fact.getRecordSchema();
		}

		@Override
		public long count() {
			return m_count;
		}
		
		@Override
		public boolean next(Record output) {
			checkNotClosed();
			
			if ( m_iter.hasNext() ) {
				output.set(m_iter.next());
				++m_count;
				return true;
			}
			
			return false;
		}
		
		@Override
		public String toString() {
			return String.format("%s[key=%s, schema={%s}]", getClass().getSimpleName(), m_groupKey, getRecordSchema());
		}
	}
}
