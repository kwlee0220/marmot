package marmot.support;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.RecordSetFunction;
import marmot.optor.support.AbstractRecordSetFunction;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LazyRecordSetFunction extends AbstractRecordSetFunction {
	private final RecordSetFunction m_optor;
	
	public LazyRecordSetFunction(RecordSetFunction consumer) {
		m_optor = consumer;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		m_optor.initialize(m_marmot, inputSchema);
		
		setInitialized(marmot, inputSchema, m_optor.getRecordSchema());
	}

	@Override
	public RecordSet apply(RecordSet input) {
		checkInitialized();
		
		return m_optor.apply(input);
	}
}
