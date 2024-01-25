package marmot.optor.join;

import marmot.Record;
import marmot.RecordSchema;
import marmot.io.RecordWritable;
import marmot.rset.AbstractRecordSet;
import marmot.rset.PushBackableRecordSet;
import marmot.support.DefaultRecord;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class DecodedRecordSet extends AbstractRecordSet {
	private final PushBackableRecordSet m_src;
	private final RecordSchema m_schema;
	private final Record m_encoded;
	
	DecodedRecordSet(PushBackableRecordSet src, RecordSchema schema) {
		m_src = src;
		m_schema = schema;
		m_encoded = DefaultRecord.of(src.getRecordSchema());
	}
	
	@Override protected void closeInGuard() { }

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	@Override
	public boolean next(Record output) {
		if ( m_src.next(m_encoded) ) {
			byte[] bytes = (byte[])m_encoded.get(MapReduceHashJoin.TAG_IDX_DECODED);
			RecordWritable.fromBytes(bytes, output);
			
			return true;
		}
		else {
			return false;
		}
	}
}