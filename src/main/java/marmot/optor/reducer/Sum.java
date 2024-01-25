package marmot.optor.reducer;

import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.optor.AggregateType;
import marmot.support.DataUtils;
import marmot.type.DataType;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Sum implements ValueAggregate {
	private static final String DEFAULT_OUT_COLUMN = "sum";
	
	private final String m_aggrColName;
	private String m_outColName = DEFAULT_OUT_COLUMN;
	
	private Column m_inputCol = null;
	private DataType m_intermType;
	private int m_intermColIdx = -1;

	public Sum(String colName) {
		Utilities.checkNotNullArgument(colName, "aggregate input column");
		
		m_aggrColName = colName;
	}

	@Override
	public AggregateType getAggregateType() {
		return AggregateType.SUM;
	}

	@Override
	public String getAggregateColumn() {
		return m_aggrColName;
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
		m_inputCol = inputSchema.getColumn(m_aggrColName);
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
		DataType type = computeOutputType(m_intermType);
		return RecordSchema.builder()
							.addColumn(m_outColName, type)
							.build();
	}

	@Override
	public void toIntermediate(Record input, Record intermediate) {
		Object value = input.get(m_inputCol.ordinal());

		Object interm;
		switch ( m_intermType.getTypeCode() ) {
			case LONG:
				interm = DataUtils.asLong(value);
				break;
			case DOUBLE:
				 interm = DataUtils.asDouble(value);
				break;
			default:
				throw new AssertionError("invalid SUM aggregate type: " + m_intermType);
		}
		
		intermediate.set(m_intermColIdx, interm);
	}

	@Override
	public void aggregate(Record accum, Record intermediate) {
		Object accumData = accum.get(m_intermColIdx);
		Object data = intermediate.get(m_intermColIdx);
		
		switch ( m_intermType.getTypeCode() ) {
			case LONG:
				accumData = (accumData == null)
							? (long)data : (long)accumData + (long)data;
				break;
			case DOUBLE:
				accumData = (accumData == null)
							? (double)data : (double)accumData + (double)data;
				break;
			default:
				throw new AssertionError("invalid SUM aggregate type: " + m_intermType);
		}
		accum.set(m_intermColIdx, accumData);
	}

	@Override
	public void toFinal(Record accum, Record output) {
		Object accumData = accum.get(m_intermColIdx);
		
		Column col = output.getRecordSchema().getColumn(m_outColName);
		output.set(col.ordinal(), accumData);
	}
	
	@Override
	public String toString() {
		return String.format("sum[%s->%s]", m_aggrColName, m_outColName);
	}
	
	private DataType computeIntermediateType(DataType inputDataType) {
		switch ( inputDataType.getTypeCode() ) {
			case BYTE:
			case SHORT:
			case INT:
			case LONG:
				return DataType.LONG;
			case FLOAT:
			case DOUBLE:
				return DataType.DOUBLE;
			default:
				throw new IllegalArgumentException("unsupported 'sum' data type: " + inputDataType);
		}
	}
	
	private DataType computeOutputType(DataType intermType) {
		switch ( intermType.getTypeCode() ) {
			case LONG:
				return DataType.LONG;
			case DOUBLE:
				return DataType.DOUBLE;
			default:
				throw new IllegalArgumentException("unsupported 'sum' data type: " + intermType);
		}
	}
}
