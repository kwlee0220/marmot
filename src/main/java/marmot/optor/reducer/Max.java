package marmot.optor.reducer;

import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.optor.AggregateType;
import marmot.type.DataType;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Max implements ValueAggregate {
	private static final String DEFAULT_OUT_COLUMN = "max";
	
	private final String m_colName;
	private String m_outColName = DEFAULT_OUT_COLUMN;
	
	private Column m_inputCol = null;
	private DataType m_intermType;
	private int m_intermColIdx = -1;
	
	public Max(String colName) {
		m_colName = colName;
	}

	@Override
	public AggregateType getAggregateType() {
		return AggregateType.MAX;
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
		
		m_outColName = colName.toLowerCase();
	}

	@Override
	public void initializeWithInput(RecordSchema inputSchema) {
		m_inputCol = inputSchema.getColumn(m_colName);
		m_intermType = computeIntermediateType(m_inputCol.type());
	}

	@Override
	public void setIntermediateSchema(RecordSchema intermediateSchema) {
		m_intermColIdx = intermediateSchema.getColumn(m_outColName).ordinal();
	}

	@Override
	public void initializeWithIntermediate(RecordSchema intermediateSchema) {
		Column intermCol = intermediateSchema.getColumn(m_outColName);
		
		m_intermColIdx = intermCol.ordinal();
		m_intermType = intermCol.type();
	}

	@Override
	public RecordSchema getIntermediateValueSchema() {
		return RecordSchema.builder()
							.addColumn(m_outColName, m_intermType)
							.build();
	}

	@Override
	public RecordSchema getOutputValueSchema() {
		return RecordSchema.builder()
							.addColumn(m_outColName, m_intermType)
							.build();
	}

	@Override
	public void toIntermediate(Record input, Record intermediate) {
		Object value = input.get(m_inputCol.ordinal());
		intermediate.set(m_intermColIdx, value);
	}

	@Override
	public void aggregate(Record accum, Record intermediate) {
		Object max = accum.get(m_intermColIdx);
		Object data = intermediate.get(m_intermColIdx);
		if ( max == null || ((Comparable<Object>)max).compareTo(data) < 0 ) {
			accum.set(m_intermColIdx, data);
		}
	}

	@Override
	public void toFinal(Record accum, Record output) {
		Object max = accum.get(m_intermColIdx);
		
		Column col = output.getRecordSchema().getColumn(m_outColName);
		output.set(col.ordinal(), max);
	}
	
	@Override
	public String toString() {
		return String.format("max[%s->%s]",
							(m_inputCol == null) ? "?" : m_inputCol.name(),
							m_outColName);
	}

	private DataType computeIntermediateType(DataType inputDataType) {
		switch ( inputDataType.getTypeCode() ) {
			case STRING:
				return DataType.STRING;
			case BYTE:
				return DataType.BYTE;
			case SHORT:
				return DataType.SHORT;
			case INT:
				return DataType.INT;
			case LONG:
				return DataType.LONG;
			case FLOAT:
				return DataType.FLOAT;
			case DOUBLE:
				return DataType.DOUBLE;
			default:
				throw new IllegalArgumentException("unsupported 'max' data type: " + inputDataType);
		}
	}
}
