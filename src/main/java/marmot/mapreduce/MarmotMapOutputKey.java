package marmot.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import org.apache.hadoop.io.WritableComparable;

import com.google.common.base.Preconditions;

import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.io.MultiColumnKey;
import marmot.io.serializer.DataTypeSerializer;
import marmot.io.serializer.MarmotSerializers;
import marmot.optor.KeyColumn;
import marmot.optor.NullsOrder;
import marmot.optor.SortOrder;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotMapOutputKey implements WritableComparable<MarmotMapOutputKey> {
	private static final byte MAX_GROUP_KEY_LENGTH = Byte.MAX_VALUE;
	private static final byte MAX_KEY_LENGTH = Byte.MAX_VALUE;
	
	private short m_groupKeyLength;
	private Object[] m_values;
	private SortOrder[] m_orders;
	private NullsOrder[] m_nullsOrders;
	@SuppressWarnings("rawtypes")
	private DataTypeSerializer[] m_serializers;
	
	// 데이터 레코드에서 키 값에 해당하는 컬럼들을 신속히 로드 (컬럼 인덱스를 통한 로드)하기
	// 위해 데이터 레코드 내 키에 해당하는 컬럼들에 대한 정보.
	// 이것은 한 GroupKeyValue는 동일 스키마를 갖는 데이터 레코드에 키 값으로만
	// 사용한다는 것을 가정하기 때문에, 여러 스키마를 갖는 레코드의 키 값용으로
	// 사용하는 경우 오동작을 하게 된다.
	private Column[] m_keyCols = null;
	
//	public static MarmotMapOutputKey from(MultiColumnKey groupKey, Record record) {
//		MarmotMapOutputKeyColumns keyCols = MarmotMapOutputKeyColumns.fromGroupKey(groupKey);
//		return new MarmotMapOutputKey().load(keyCols, record);
//	}
	
	public MarmotMapOutputKey() { }
	
	MarmotMapOutputKey(int groupKeyLength, MultiColumnKey key, RecordSchema schema) {
		int length = key.length();
		
		Utilities.checkArgument(groupKeyLength <= MAX_GROUP_KEY_LENGTH && groupKeyLength >= 0,
								() -> "invalid groupKeyLength: " + groupKeyLength);
		Utilities.checkArgument(length <= MAX_KEY_LENGTH && length >= groupKeyLength,
								() -> "invalid keyLength: " + length);
		
		m_groupKeyLength = (short)groupKeyLength;
		m_serializers = new DataTypeSerializer[length];
		m_values = new Object[length];
		m_orders = new SortOrder[length];
		Arrays.fill(m_orders, SortOrder.ASC);
		m_nullsOrders = new NullsOrder[length];
		Arrays.fill(m_nullsOrders, NullsOrder.FIRST);
		
		m_keyCols = new Column[length];
		for ( int i =0; i < length; ++i ) {
			final KeyColumn keyCol = key.getKeyColumnAt(i);
			
			m_keyCols[i] = schema.getColumn(keyCol.name());
			m_serializers[i] = MarmotSerializers.getSerializer(m_keyCols[i].type());
			if ( i >= groupKeyLength ) {
				m_orders[i] = keyCol.sortOrder();
				m_nullsOrders[i] = keyCol.nullsOrder();
			}
		}
	}
	
	public MarmotMapOutputKey load(Record record) {
		for ( int i =0; i < m_values.length; ++i ) {
			m_values[i] = record.get(m_keyCols[i].ordinal());
		}
		
		return this;
	}
	
	public Object[] getValueAll() {
		return m_values;
	}
	
	public Object[] getGroupKeys() {
		return Arrays.copyOf(m_values, m_groupKeyLength);
	}

	@Override
	public int compareTo(MarmotMapOutputKey otherKey) {
		return compare(otherKey, m_values.length);
	}
	
	public int compareGroupTo(MarmotMapOutputKey otherKey) {
		Preconditions.checkArgument(otherKey.m_groupKeyLength == m_groupKeyLength);
		
		return compare(otherKey, m_groupKeyLength);
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(getGroupKeys());
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || getClass() != obj.getClass() ) {
			return false;
		}
		
		MarmotMapOutputKey other = (MarmotMapOutputKey)obj;
		return compareTo(other) == 0;
	}
	
	// 1. (short) group key length
	// 2. (short) key length
	// 3. for each key columns
	//	3.1. (byte) compare order
	//	3.2. (byte) nulls order
	//	3.3. (byte) type code
	//  3.4  (????) values

	@Override
	public void readFields(DataInput in) throws IOException {
		m_groupKeyLength = in.readShort();
		if ( m_groupKeyLength > MAX_GROUP_KEY_LENGTH || m_groupKeyLength < 0 ) {
			String details = String.format("MapOutputKey data is corrupted: group_key_length=%d",
											m_groupKeyLength);
			throw new IOException(details);
		}
		
		short length = in.readShort();
		if ( length > MAX_KEY_LENGTH || length < m_groupKeyLength ) {
			String details = String.format("MapOutputKey data is corrupted: "
											+ "group_key_length=%d, key_length=%d",
											m_groupKeyLength, length);
			throw new IOException(details);
		}
		
		if ( m_serializers == null || m_serializers.length != length) {
			m_serializers = new DataTypeSerializer[length];
			m_values = new Object[length];
			m_orders = new SortOrder[length];
			m_nullsOrders = new NullsOrder[length];
		}
		
		for ( int i =0; i < length; ++i ) {
			m_orders[i] = SortOrder.values()[in.readByte()];
			m_nullsOrders[i] = NullsOrder.values()[in.readByte()];
			
			int tc = MarmotSerializers.readNullableTypeCode(in);
			if ( tc > 0 ) {	// valid (non-null)
				m_serializers[i] = MarmotSerializers.getSerializer(tc);
				m_values[i] = m_serializers[i].deserialize(in);
			}
			else {
				m_serializers[i] = MarmotSerializers.getSerializer(-tc);
				m_values[i] = null;
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeShort(m_groupKeyLength);
		out.writeShort(m_values.length);
		for ( int i =0; i < m_values.length; ++i ) {
			out.writeByte(m_orders[i].ordinal());
			out.writeByte(m_nullsOrders[i].ordinal());
			
			int tc = m_serializers[i].getDataType().getTypeCode().get();
			Object value = m_values[i];
			if ( value != null ) {
				out.writeByte(tc);
				m_serializers[i].serialize(value, out);
			}
			else {
				out.writeByte(-tc);
			}
		}
	}
	
	@Override
	public String toString() {
		if ( m_values == null ) {
			return "unknown";
		}
		
		StringBuilder builder = new StringBuilder();
		if ( m_groupKeyLength > 0 ) {
			builder.append('{');
			
			for ( int i = 0; i < m_groupKeyLength; ++i ) {
				Object v = m_values[i];
				builder.append((v != null) ? v : "?").append(',');
			}
			builder.setLength(builder.length()-1);
			builder.append("},");
		}
		for ( int i = m_groupKeyLength; i < m_values.length; ++i ) {
			Object v = m_values[i];
			builder.append((v != null) ? v : "?")
					.append(':').append(m_orders[i])
					.append(',');
		}
		
		builder.setLength(builder.length()-1);
		return builder.toString();
	}
	
	private int compare(MarmotMapOutputKey otherKey, int length) {
		if ( this == otherKey ) {
			return 0;
		}
		
		for ( int i =0; i < length; ++i ) {
			final Object obj = m_values[i];
			final Object other = otherKey.m_values[i];
			SortOrder order = m_orders[i];

			if ( obj != null && other != null ) {
				// GroupKeyValue에 올 수 있는 객체는 모두 Comparable 이라는 것을 가정한다.
				@SuppressWarnings({ "rawtypes", "unchecked" })
				int cmp = ((Comparable)obj).compareTo(other);
				if ( cmp != 0 ) {
					return (order == SortOrder.ASC) ? cmp : -cmp;
				}
			}
			else if ( obj == null && other == null ) { }
			else if ( obj == null ) {
				if ( order == SortOrder.ASC ) {
					return (m_nullsOrders[i] == NullsOrder.FIRST) ? -1 : 1;
				}
				else {
					return (m_nullsOrders[i] == NullsOrder.FIRST) ? 1 : -1;
				}
			}
			else if ( other == null ) {
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
	
/*
	
	public static GroupMapOutputKey from(MultiColumnKey groupKey, MultiColumnKey orderKey, Record record) {
		return new GroupMapOutputKey().load(groupKey, orderKey, record);
	}
	
	public GroupMapOutputKey load(MultiColumnKey key, Record record) {
		if ( m_keyCols == null || m_keyCols.length != key.length() ) {
			m_serdes = new MarmotSerDe[key.length()];
			m_values = new Object[key.length()];
			
			int i =0;
			m_keyCols = new Column[key.length()];
			RecordSchema schema = record.getSchema();
			for ( KeyColumn keyCol: key.getKeyColumnAll() ) {
				m_keyCols[i] = schema.getColumn(keyCol.name());
				++i;
			}
		}
		
		for ( int i =0; i < key.length(); ++i ) {
			final Column col = m_keyCols[i];
			
			m_serdes[i] = MarmotSerDes.getSerDe(col.getType());
			m_values[i] = record.get(col.ordinal());
			if ( m_values[i] == null ) {
				throw new IllegalArgumentException("group key cannot be null: " + this);
			}
		}
		
		return this;
	}
	
	public GroupMapOutputKey load(MultiColumnKey groupKey, MultiColumnKey orderKey, Record record) {
		int keyLen = groupKey.length() + orderKey.length();
		if ( m_keyCols == null || m_keyCols.length != keyLen ) {
			m_serdes = new MarmotSerDe[keyLen];
			m_values = new Object[keyLen];
			
			int i =-1;
			m_keyCols = new Column[keyLen];
			RecordSchema schema = record.getSchema();
			for ( KeyColumn keyCol: groupKey.getKeyColumnAll() ) {
				m_keyCols[++i] = schema.getColumn(keyCol.getName());
			}
			for ( KeyColumn keyCol: orderKey.getKeyColumnAll() ) {
				m_keyCols[++i] = schema.getColumn(keyCol.name());
			}
		}
		
		for ( int i =0; i < keyLen; ++i ) {
			final Column col = m_keyCols[i];
			
			m_serdes[i] = MarmotSerDes.getSerDe(col.getType());
			m_values[i] = record.get(col.ordinal());
			if ( m_values[i] == null ) {
				throw new IllegalArgumentException("group key cannot be null: " + this);
			}
		}
		
		return this;
	}
	
	public int getGroupKeyLength() {
		return m_groupKeyLength;
	}

	static {
	    WritableComparator.define(GroupMapOutputKey.class, new Comparator());
	}
	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(GroupMapOutputKey2.class);
		}
		
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			Cursor cursor = new Cursor(b1, s1, b2, s2);
			
			int nvals = cursor.readUnsignedShort();
			for ( int i =0; i < nvals; ++i ) {
				DataType type = DataTypes.fromTypeCode(cursor.readByte());
				MarmotSerDe serde = MarmotSerDes.getSerDe(type);
				if ( serde instanceof ComparableMarmotSerDe ) {
					int cmp = ((ComparableMarmotSerDe)serde).compareBytes(cursor);
					if ( cmp != 0 ) {
						return cmp;
					}
				}
				else {
					throw new IllegalStateException("Unsupported raw-comparision DataType: "
													+ "type=" + type);
				}
			}
			
			return 0;
		}
	}
*/
}
