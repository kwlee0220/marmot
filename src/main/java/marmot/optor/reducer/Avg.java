package marmot.optor.reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.io.serializer.MarmotSerializers;
import marmot.optor.AggregateType;
import marmot.support.TypedObject;
import marmot.type.DataType;
import marmot.type.TypeCode;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Avg implements ValueAggregate {
	private static final String DEFAULT_OUT_COLUMN = "avg";
	
	private final String m_colName;
	private String m_outColName = DEFAULT_OUT_COLUMN;
	
	private Column m_inputCol = null;
	private int m_intermColIdx = -1;
	
	public Avg(String colName) {
		m_colName = colName;
	}

	@Override
	public AggregateType getAggregateType() {
		return AggregateType.AVG;
	}

	@Override
	public String getAggregateColumn() {
		return m_colName;
	}

	@Override
	public String getOutputColumn() {
		return m_outColName;
	}

	@Override
	public void setOutputColumn(String colName) {
		Utilities.checkNotNullArgument(colName, "colName is null");
		
		m_outColName = colName;
	}

	@Override
	public void initializeWithInput(RecordSchema inputSchema) {
		m_inputCol = inputSchema.getColumn(m_colName);
	}

	@Override
	public void setIntermediateSchema(RecordSchema intermediateSchema) {
		m_intermColIdx = intermediateSchema.getColumn(m_outColName).ordinal();
	}

	@Override
	public void initializeWithIntermediate(RecordSchema intermediateSchema) {
		m_intermColIdx = intermediateSchema.getColumn(m_outColName).ordinal();
	}

	@Override
	public RecordSchema getIntermediateValueSchema() {
		return RecordSchema.builder()
							.addColumn(m_outColName, DataType.TYPED)
							.build();
	}

	@Override
	public RecordSchema getOutputValueSchema() {
		return RecordSchema.builder()
							.addColumn(m_outColName, DataType.DOUBLE)
							.build();
	}

	@Override
	public void toIntermediate(Record input, Record intermediate) {
		Object value = input.get(m_inputCol.ordinal());
		
		Accum accum;
		switch ( m_inputCol.type().getTypeCode() ) {
			case BYTE:
				accum = new Accum((byte)value);
				break;
			case SHORT:
				accum = new Accum((short)value);
				break;
			case INT:
				accum = new Accum((int)value);
				break;
			case LONG:
				accum = new Accum((long)value);
				break;
			case DOUBLE:
				accum = new Accum((double)value);
				break;
			case FLOAT:
				accum = new Accum((float)value);
				break;
			default:
				throw new IllegalArgumentException("unsupported DataType for Avg: column="
													+ m_inputCol.type());
		}
		intermediate.set(m_intermColIdx, accum);
	}

	@Override
	public void aggregate(Record accum, Record intermediate) {
		Accum a = (Accum)accum.get(m_intermColIdx);
		Accum data = (Accum)intermediate.get(m_intermColIdx);
		if ( a == null ) {
			accum.set(m_intermColIdx, data);
		}
		else {
			a.combine(data);
		}
	}

	@Override
	public void toFinal(Record accum, Record output) {
		Accum a = (Accum)accum.get(m_intermColIdx);
		
		Column col = output.getRecordSchema().getColumn(m_outColName);
		output.set(col.ordinal(), a.getFinal());
	}
	
	@Override
	public String toString() {
		return String.format("avg[%s->%s]",
							(m_inputCol == null) ? "?" : m_inputCol.name(),
							m_outColName);
	}
	
	private static final class Accum implements TypedObject {
		private TypeCode m_sumType;
		private long m_lsum;
		private double m_dsum;
		private long m_count;
		
		private Accum(long init) {
			m_sumType = TypeCode.LONG;
			m_lsum = init;
			m_count = 1;
		}
		
		private Accum(double init) {
			m_sumType = TypeCode.DOUBLE;
			m_dsum = init;
			m_count = 1;
		}
		
		Object getFinal() {
			switch ( m_sumType ) {
				case LONG:
					return m_lsum / (double)m_count;
				case DOUBLE:
					return m_dsum / m_count;
				default:
					throw new AssertionError("invalid SUM aggregate type: " + m_sumType);
			}
		}
		
		void combine(Accum accum) {
			switch ( m_sumType ) {
				case LONG:
					m_lsum += accum.m_lsum;
					break;
				case DOUBLE:
					m_dsum += accum.m_dsum;
					break;
				default:
					throw new AssertionError("invalid SUM aggregate type: " + m_sumType);
			}
			m_count += accum.m_count;
		}
		
		@Override
		public String toString() {
			switch ( m_sumType ) {
				case LONG:
					return "" + m_lsum + "/" + m_count;
				case DOUBLE:
					return "" + m_dsum + "/" + m_count;
				default:
					throw new AssertionError("invalid SUM aggregate type: " + m_sumType);
			}
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			m_sumType = MarmotSerializers.readTypeCode(in);
			m_count = in.readLong();
			switch ( m_sumType ) {
				case LONG:
					m_lsum = in.readLong();
					break;
				case DOUBLE:
					m_dsum = in.readDouble();
					break;
				default:
					throw new AssertionError("invalid SUM aggregate type: " + m_sumType);
			}
		}

		@Override
		public void writeFields(DataOutput out) throws IOException {
			out.writeByte(m_sumType.get());
			out.writeLong(m_count);
			switch ( m_sumType ) {
				case LONG:
					out.writeLong(m_lsum);
					break;
				case DOUBLE:
					out.writeDouble(m_dsum);
					break;
				default:
					throw new AssertionError("invalid SUM aggregate type: " + m_sumType);
			}
		}
	}
}
