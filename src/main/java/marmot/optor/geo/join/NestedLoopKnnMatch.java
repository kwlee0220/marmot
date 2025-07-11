package marmot.optor.geo.join;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;

import utils.Tuple;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class NestedLoopKnnMatch {
	private final Record m_outer;
	private final RecordSchema m_innerSchema;
	private final FStream<Tuple<Record,Double>> m_inners;
	
	public NestedLoopKnnMatch(Record outer, RecordSchema innerSchema,
							FStream<Tuple<Record,Double>> inners) {
		m_outer = outer;
		m_innerSchema = innerSchema;
		m_inners = inners;
	}
	
	/**
	 * 매치된 outer 레코드를 반환한다.
	 * 
	 * @return	레코드
	 */
	public Record getOuterRecord() {
		return m_outer;
	}
	
	/**
	 * Outer 레코드와 매치된 inner 레코드들의 리스트를 반환한다.
	 * 
	 * @return	레코드 리스트.
	 */
	public FStream<Tuple<Record,Double>> getInnerRecords() {
		return m_inners;
	}
	
	/**
	 * Outer 레코드와 매치된 inner 레코드들의 스키마를 반환한다.
	 * 
	 * @return	레코드 스키마.
	 */
	public RecordSchema getInnerRecordSchema() {
		return m_innerSchema;
	}
	
	public RecordSet getInnerRecordSet() {
		return RecordSet.from(m_innerSchema, m_inners.map(Tuple::_1));
	}
}