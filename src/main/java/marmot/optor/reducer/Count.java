package marmot.optor.reducer;

import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.optor.AggregateFunction;
import marmot.optor.AggregateType;
import marmot.type.DataType;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Count implements ValueAggregate {
	private String m_outColName;
	
	private int m_intermColIdx = -1;

	public Count(String aggrCol) {
		m_outColName = AggregateFunction.COUNT().m_resultColumn;
	}

	@Override
	public AggregateType getAggregateType() {
		return AggregateType.COUNT;
	}

	@Override
	public String getAggregateColumn() {
		return null;
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
							.addColumn(m_outColName, DataType.LONG)
							.build();
	}

	@Override
	public RecordSchema getOutputValueSchema() {
		return RecordSchema.builder()
							.addColumn(m_outColName, DataType.LONG)
							.build();
	}

	@Override
	public void toIntermediate(Record input, Record intermediate) {
		intermediate.set(m_intermColIdx, 1L);
	}

	@Override
	public void aggregate(Record accum, Record intermediate) {
		long accumData = accum.getLong(m_intermColIdx);
		long data = intermediate.getLong(m_intermColIdx);
		
		accum.set(m_intermColIdx, accumData + data);
	}

	@Override
	public void toFinal(Record accum, Record output) {
		long accumData = accum.getLong(m_intermColIdx);
		
		Column col = output.getRecordSchema().getColumn(m_outColName);
		output.set(col.ordinal(), accumData);
	}
	
	@Override
	public String toString() {
		return String.format("count[->%s]", m_outColName);
	}
}
