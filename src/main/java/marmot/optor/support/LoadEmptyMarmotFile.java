package marmot.optor.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;


/**
 * {@code LoadMarmotFile}연산은 지정된 HDFS 파일 (또는 복수개의 HDFS 파일들)을 읽어 저장된
 * 레코드를 읽어 레코드 세트로 생성하는 작업을 수행한다.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LoadEmptyMarmotFile extends AbstractRecordSetLoader {
	static final Logger s_logger = LoggerFactory.getLogger(LoadEmptyMarmotFile.class);

	private final RecordSchema m_schema;
	
	public LoadEmptyMarmotFile(RecordSchema schema) {
		m_schema = schema;
	}

	@Override
	public void initialize(MarmotCore marmot) {
		setInitialized(marmot, m_schema);
	}

	@Override
	public RecordSet load() {
		return RecordSet.empty(m_schema);
	}
	
	@Override
	public String toString() {
		return "load_empty_file";
	}
}
