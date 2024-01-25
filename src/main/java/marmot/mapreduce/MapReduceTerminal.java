package marmot.mapreduce;

import marmot.Plan;


/**
 * MapReduce job에서 마지막 task의 인터페이스를 정의한다.
 * MapReduce 방식으로 처리될 {@link Plan}의 마지막 연산자는
 * 반드시 {@code MapRedeceTerminal} 인터페이스를 구현하여야 한다.
 * 
 * {@code MapRedeceTerminal} 이후에 레코드 처리기들은 새로운 MapReduce 작업에서 수행되어야 한다.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface MapReduceTerminal {
}
