package marmot.optor.rset;

import org.slf4j.Logger;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.io.RecordKey;
import marmot.optor.support.KeyedRecordSet;
import marmot.support.ProgressReportable;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SimpleKeyedRecordSet implements KeyedRecordSet, ProgressReportable {
	private final RecordKey m_key;
	private final RecordSchema m_keySchema;
	private final RecordSet m_rset;
	
	public SimpleKeyedRecordSet(RecordKey key, RecordSchema keySchema, RecordSet rset) {
		m_key = key;
		m_keySchema = keySchema;
		m_rset = rset;
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_rset.getRecordSchema();
	}

	@Override
	public void close() {
		m_rset.close();
	}

	@Override
	public RecordKey getKey() {
		return m_key;
	}

	@Override
	public RecordSchema getKeySchema() {
		return m_keySchema;
	}

	@Override
	public boolean next(Record output) {
		return m_rset.next(output);
	}

	@Override
	public Record nextCopy() {
		return m_rset.nextCopy();
	}
	
	@Override
	public void reportProgress(Logger logger, StopWatch elapsed) {
		if ( m_rset instanceof ProgressReportable ) {
			((ProgressReportable)m_rset).reportProgress(logger, elapsed);
		}
	}
}
