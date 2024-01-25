package marmot.io;

import marmot.Record;
import marmot.optor.KeyColumn;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TaggedRecordKey implements Comparable<TaggedRecordKey> {
	private final RecordKey m_key;
	private final MultiColumnKey m_tagKeys;
	private final Object[] m_tagValues;
	
	public static TaggedRecordKey from(MultiColumnKey keys, MultiColumnKey tags, Record record) {
		Object[] tagValues = tags.streamKeyColumns()
									.map(kc -> record.get(kc.name()))
									.toArray(Object.class);
		
		return new TaggedRecordKey(RecordKey.from(keys, record), tags, tagValues);
	}
	
	private TaggedRecordKey(RecordKey key, MultiColumnKey tagKeys, Object[] tagValues) {
		m_key = key;
		m_tagKeys = tagKeys;
		m_tagValues = tagValues;
	}
	
	public int length() {
		return m_key.length() + m_tagKeys.length();
	}
	
	public Object[] getValues() {
		Object[] values = new Object[length()];
		System.arraycopy(m_key.getValues(), 0, values, 0, m_key.length());
		System.arraycopy(m_tagValues, 0, values, m_key.length(), m_tagKeys.length());
		
		return values;
	}
	
	@Override
	public int compareTo(TaggedRecordKey other) {
		return m_key.compareTo(other.m_key);
	}
	
	@Override
	public int hashCode() {
		return m_key.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || getClass() != obj.getClass() ) {
			return false;
		}
		
		TaggedRecordKey other = (TaggedRecordKey)obj;
		return m_key.equals(other.m_key);
	}
	
	@Override
	public String toString() {
		String tagsStr = m_tagKeys.streamKeyColumns()
								.map(KeyColumn::name)
								.zipWith(FStream.of(m_tagValues).map(Object::toString))
								.map(t -> t._1 + ":" + t._2)
								.join(",");
		return "keys={" + m_key + "}, tags={" + tagsStr + "}";
	}
}