package marmot.optor.join;

import marmot.Record;
import marmot.RecordPredicate;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface JoinPredicate extends RecordPredicate {
	public static final String XML_NAME = "join_predicate";
	
	public void setOuterRecord(Record record);
}
