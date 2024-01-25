package marmot.optor.reducer;

import marmot.Record;
import marmot.RecordSchema;
import marmot.optor.AggregateType;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface ValueAggregate {
	public default String getName() {
		return getAggregateType().getName();
	}
	public AggregateType getAggregateType();
	
	public String getAggregateColumn();
	public String getOutputColumn();
	public void setOutputColumn(String outputColum);
	
	public default FOption<String> getParameter() {
		return FOption.empty();
	}
	
	public void initializeWithInput(RecordSchema inputSchema);
	public void setIntermediateSchema(RecordSchema intermediateSchema);
	
	public void initializeWithIntermediate(RecordSchema intermediateSchema);
	
	public RecordSchema getIntermediateValueSchema();
	public RecordSchema getOutputValueSchema();
	
	public void toIntermediate(Record input, Record intermediate);
	public void aggregate(Record accum, Record intermediate);
	public void toFinal(Record accum, Record result);
}
