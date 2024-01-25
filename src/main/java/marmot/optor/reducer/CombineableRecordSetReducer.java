package marmot.optor.reducer;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.RecordSetFunction;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.optor.support.IdentityRecordTransform;
import marmot.optor.support.RecordLevelTransform;

/**
 * {@link CombineableRecordSetReducer}는 MapReduce 과정을 통한 reduce 작업을 수행할 때,
 * map단게에서 일부 reduce 작업을 수행하고, 그 결과를 다시 combine하는 방법으로 작업을
 * 수행하는 reducer의 인터페이스를 정의한다.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class CombineableRecordSetReducer extends AbstractRecordSetFunction
													implements RecordSetReducer {
	private RecordLevelTransform m_producer;
	private RecordSetFunction m_reducer;
	private RecordSetFunction m_finalizer;
	
	/**
	 * Reduce 단계에서 전달된 중간 결과를 모아서 최종 reduce된 레코드를 생성하는 combiner를 반환한다.
	 * 
	 * @return	중간 결과용 reducer 객체.
	 */
	abstract public RecordSetReducer newIntermediateReducer();
	
	/**
	 * 입력 데이터로부터 중간 집계 데이터를 생성하는 레코드 변환기({@link RecordLevelTransform})를 반환한다.
	 * 
	 * @return	중간 결과 생산용 reducer 객체.
	 */
	public RecordLevelTransform newIntermediateProducer() {
		return new IdentityRecordTransform();
	}
	
	public RecordLevelTransform newIntermediateFinalizer() {
		return new IdentityRecordTransform();
	}
	
	protected RecordSet fromEmptyInputRecordSet() {
		return RecordSet.empty(getRecordSchema());
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		RecordSchema schema = inputSchema;
		m_producer = newIntermediateProducer();
		m_producer.initialize(marmot, schema);
		schema = m_producer.getRecordSchema();
		
		m_reducer = newIntermediateReducer();
		m_reducer.initialize(marmot, schema);
		
		m_finalizer = newIntermediateFinalizer();
		m_finalizer.initialize(marmot, schema);
		schema = m_finalizer.getRecordSchema();
		
		setInitialized(marmot, inputSchema, schema);
	}

	@Override
	public RecordSet apply(RecordSet input) {
		checkInitialized();
		
		return input.asNonEmpty()
					.map(rset -> {
						RecordSet produceds = m_producer.apply(rset);
						RecordSet interm = m_reducer.apply(produceds);
						return m_finalizer.apply(interm);
					})
					.getOrElse(() -> fromEmptyInputRecordSet());
	}
}
