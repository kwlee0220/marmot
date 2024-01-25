package marmot.optor.reducer;

import java.util.Arrays;
import java.util.Comparator;

import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.io.MultiColumnKey;
import marmot.optor.KeyColumn;
import marmot.optor.NullsOrder;
import marmot.optor.SortOrder;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class KeyedRecordComparator implements Comparator<Record> {
	private final Column[] m_keyCols;
	private final SortOrder[] m_orders;
	private final NullsOrder[] m_nullsOrders;
	
	KeyedRecordComparator(MultiColumnKey keys, RecordSchema schema) {
		m_keyCols = new Column[keys.length()];
		m_orders = new SortOrder[keys.length()];
		Arrays.fill(m_orders, SortOrder.ASC);
		m_nullsOrders = new NullsOrder[keys.length()];
		Arrays.fill(m_nullsOrders, NullsOrder.FIRST);
		
		for ( int i =0; i < keys.length(); ++i ) {
			KeyColumn kc = keys.getKeyColumnAt(i);
			
			m_keyCols[i] = schema.getColumn(kc.name());
			m_orders[i] = kc.sortOrder();
			m_nullsOrders[i] = kc.nullsOrder();
		}
	}
	
	@Override
	public int compare(Record r1, Record r2) {
		if ( r1 == r2 ) {
			return 0;
		}
		
		for ( int i =0; i < m_keyCols.length; ++i ) {
			final Object v1 = r1.get(m_keyCols[i].ordinal());
			final Object v2 = r2.get(m_keyCols[i].ordinal());
			SortOrder order = m_orders[i];

			if ( v1 != null && v2 != null ) {
				// GroupKeyValue에 올 수 있는 객체는 모두 Comparable 이라는 것을 가정한다.
				@SuppressWarnings({ "rawtypes", "unchecked" })
				int cmp = ((Comparable)v1).compareTo(v2);
				if ( cmp != 0 ) {
					return (order == SortOrder.ASC) ? cmp : -cmp;
				}
			}
			else if ( v1 == null && v2 == null ) { }
			else if ( v1 == null ) {
				if ( order == SortOrder.ASC ) {
					return (m_nullsOrders[i] == NullsOrder.FIRST) ? -1 : 1;
				}
				else {
					return (m_nullsOrders[i] == NullsOrder.FIRST) ? 1 : -1;
				}
			}
			else if ( v2 == null ) {
				if ( order == SortOrder.ASC ) {
					return (m_nullsOrders[i] == NullsOrder.FIRST) ? 1 : -1;
				}
				else {
					return (m_nullsOrders[i] == NullsOrder.FIRST) ? -1 : 1;
				}
			}
		}
		
		return 0;
	}
}
