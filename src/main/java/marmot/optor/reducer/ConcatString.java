package marmot.optor.reducer;

import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.optor.AggregateFunction;
import marmot.optor.AggregateType;
import marmot.type.DataType;
import utils.Utilities;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ConcatString implements ValueAggregate {
	private static final String DEFAULT_OUT_COLUMN = "concat";
	
	private final String m_colName;
	private final String m_delim;
	private String m_outColName = DEFAULT_OUT_COLUMN;
	
	private Column m_inputCol = null;
	private int m_intermColIdx = -1;

	public ConcatString(String inputCol, String delim) {
		m_colName = inputCol;
		m_delim = delim;
		m_outColName = AggregateFunction.CONCAT_STR(inputCol, delim).m_resultColumn;
	}

	@Override
	public AggregateType getAggregateType() {
		return AggregateType.CONCAT_STR;
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
	
	public String getDelimiter() {
		return m_delim;
	}

	@Override
	public FOption<String> getParameter() {
		return FOption.of(m_delim);
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
							.addColumn(m_outColName, DataType.STRING)
							.build();
	}

	@Override
	public RecordSchema getOutputValueSchema() {
		return RecordSchema.builder()
							.addColumn(m_outColName, DataType.STRING)
							.build();
	}

	@Override
	public void toIntermediate(Record input, Record intermediate) {
		Object value = input.get(m_inputCol.ordinal());
		intermediate.set(m_intermColIdx, value.toString());
	}

	@Override
	public void aggregate(Record accum, Record intermediate) {
		String accumData = accum.getString(m_intermColIdx);
		String data = intermediate.getString(m_intermColIdx);

		accumData = (accumData != null) ? accumData + m_delim + data : data;
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
		return String.format("%s(%s,delim='%s',out=%s)",
							AggregateType.CONCAT_STR, m_colName, m_delim, m_outColName);
	}
}
