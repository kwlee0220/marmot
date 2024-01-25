package marmot.mapreduce;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.ReduceContext;
import org.slf4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSetException;
import marmot.io.MultiColumnKey;
import marmot.io.RecordKey;
import marmot.io.RecordWritable;
import marmot.optor.support.KeyedRecordSet;
import marmot.optor.support.KeyedRecordSetFactory;
import marmot.rset.AbstractRecordSet;
import marmot.support.DefaultRecord;
import marmot.support.ProgressReportable;
import marmot.support.RecordProjector;
import utils.StopWatch;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ReduceContextKeyedRecordSetFactory implements KeyedRecordSetFactory {
	@SuppressWarnings("rawtypes")
	private final ReduceContext m_context;
	private final MultiColumnKey m_keyCols;
	private final MultiColumnKey m_tagCols;
	private final MultiColumnKey m_groupKeyCols;
	private final RecordSchema m_groupKeySchema;
	private final RecordProjector m_valueProjector;
	private final RecordSchema m_inputSchema;
	private final RecordSchema m_outSchema;

	private Iterator<RecordWritable> m_cursor = Collections.emptyIterator();
	private int m_groupCount = 0;
	
	@SuppressWarnings("rawtypes")
	public ReduceContextKeyedRecordSetFactory(ReduceContext context, RecordSchema inputSchema,
											MultiColumnKey keyCols, MultiColumnKey tagCols) {
		m_context = context;
		m_inputSchema = inputSchema;
		
		m_keyCols = keyCols;
		m_tagCols = tagCols;
		m_groupKeyCols = MultiColumnKey.concat(keyCols, tagCols);
		m_groupKeySchema = inputSchema.project(m_groupKeyCols.getColumnNames());

		MultiColumnKey valueCols = m_groupKeyCols.complement(inputSchema);
		m_valueProjector = RecordProjector.of(inputSchema, valueCols.getColumnNames());
		m_outSchema = m_valueProjector.getOutputRecordSchema();
	}
	
	@Override
	public RecordSchema getRecordSchema() {
		return m_outSchema;
	}

	@Override
	public MultiColumnKey getKeyColumns() {
		return m_groupKeyCols;
	}

	@Override
	public RecordSchema getKeySchema() {
		return m_groupKeySchema;
	}

	@Override
	public void close() {
		if ( m_cursor != null ) {
			m_cursor = null;
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public FOption<KeyedRecordSet> nextKeyedRecordSet() {
		Preconditions.checkState(m_cursor != null, "closed already");
		
		try {
			if ( m_cursor instanceof ReduceContext.ValueIterator ) {
				((ReduceContext.ValueIterator)m_cursor).resetBackupStore();
			}
			
			if ( !m_context.nextKey() ) {
				return FOption.empty();
			}
			++m_groupCount;
			
			return FOption.of(new ReduceSideGroupRecordSet(m_context.getValues().iterator()));
		}
		catch ( IOException | InterruptedException e ) {
			throw new RecordSetException(e);
		}
	}

	@Override
	public void reportProgress(Logger logger, StopWatch elapsed) {
		logger.info("report: [{}]{}", m_cursor == null ? "C": "O", toString());
	}
	
	@Override
	public String toString() {
		String tagsStr = (m_tagCols.length() > 0)
						? String.format(", tags={%s}", m_tagCols) : "";
		return String.format("reduce_side_groups[keys={%s}%s]: ngroups=%d",
							m_keyCols, tagsStr, m_groupCount);
	}

	private class ReduceSideGroupRecordSet extends AbstractRecordSet
											implements KeyedRecordSet, ProgressReportable {
		private final RecordKey m_key;
		private final PeekingIterator<RecordWritable> m_iter;
		private final Record m_inputRecord;
		private long m_count = 0;
		
		private boolean m_finalProgressReported = false;
		
		ReduceSideGroupRecordSet(Iterator<RecordWritable> iter) {
			m_inputRecord = DefaultRecord.of(m_inputSchema);
			
			m_iter = Iterators.peekingIterator(iter);
			m_iter.peek().storeTo(m_inputRecord);
			m_key = RecordKey.from(m_groupKeyCols, m_inputRecord);
		}
		
		@Override protected void closeInGuard() { }

		@Override
		public RecordKey getKey() {
			return m_key;
		}

		@Override
		public RecordSchema getKeySchema() {
			return m_groupKeySchema;
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_outSchema;
		}

		@Override
		public long count() {
			return m_count;
		}

		@Override
		public boolean next(Record output) throws RecordSetException {
			checkNotClosed();
			
			if ( m_iter.hasNext() ) {
				m_iter.next().storeTo(m_inputRecord);
				m_valueProjector.apply(m_inputRecord, output);
				++m_count;
				
				return true;
			}
			else {
				return false;
			}
		}
		
		@Override
		public String toString() {
			return String.format("%s: key=%s, schema={%s}",
								getClass().getSimpleName(), getKey(), getRecordSchema());
		}
		
		@Override
		public void reportProgress(Logger logger, StopWatch elapsed) {
			if ( !isClosed() || !m_finalProgressReported ) {
				logger.info("report: [{}]{}", isClosed() ? "C": "O", toString());
				
				if ( isClosed() ) {
					m_finalProgressReported = true;
				}
			}
		}
	}
}