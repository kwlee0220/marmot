package marmot;

import java.util.Comparator;

import marmot.io.MultiColumnKey;
import marmot.io.RecordKey;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordComparator implements Comparator<Record> {
	private MultiColumnKey m_keyCols;
	
	public RecordComparator(MultiColumnKey keyCols) {
		m_keyCols = keyCols;
	}

	@Override
	public int compare(Record record1, Record record2) {
		RecordKey key1 = RecordKey.from(m_keyCols, record1);
		RecordKey key2 = RecordKey.from(m_keyCols, record2);
		
		return key1.compareTo(key2);
	}
}