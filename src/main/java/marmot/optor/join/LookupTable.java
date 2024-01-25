package marmot.optor.join;

import java.util.Set;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.io.MultiColumnKey;
import marmot.io.RecordKey;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface LookupTable {
	public interface Visitor {
		public void visit(Record record);
	}
	
	public void open(MarmotCore marmot);
	public void close();
	
	public MultiColumnKey getKeyColumns();
	public Set<RecordKey> getDistinctKeys();
	
	public RecordSchema getRecordSchema();
	public RecordSet lookup(Object[] keyValues);
}
