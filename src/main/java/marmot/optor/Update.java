package marmot.optor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordScript;
import marmot.RecordSetException;
import marmot.optor.support.ColumnVariableResolverFactory;
import marmot.optor.support.RecordLevelTransform;
import marmot.proto.optor.UpdateProto;
import marmot.support.PBSerializable;
import marmot.support.RecordScriptExecution;
import utils.Throwables;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Update extends RecordLevelTransform implements PBSerializable<UpdateProto> {
	static final Logger s_logger = LoggerFactory.getLogger(Update.class);

	private final RecordScript m_script;
	private final RecordScriptExecution m_updateExec;

	// MVEL 스크립트 처리시 레코드의 각 컬럼 값과 기타 임시 변수 값을 기록.
	private ColumnVariableResolverFactory m_vrFact;
	
	public Update(String updateExpr) {
		this(RecordScript.of(updateExpr));
	}
	
	private Update(RecordScript script) {
		Utilities.checkNotNullArgument(script, "update script is null");
		
		m_script = script;
		m_updateExec = RecordScriptExecution.of(script);
		setLogger(s_logger);
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		m_vrFact = new ColumnVariableResolverFactory(inputSchema, m_script.getArgumentAll());
		m_updateExec.initialize(m_vrFact);
		
		setInitialized(marmot, inputSchema, inputSchema);
	}
	
	@Override
	public boolean transform(Record input, Record output) {
		m_vrFact.bind(input);
		
		try {
			m_updateExec.execute(m_vrFact);
			output.set(input);
			
			return true;
		}
		catch ( Throwable e ) {
			e = Throwables.unwrapThrowable(e);
			Throwables.throwIfInstanceOf(e, RecordSetException.class);
			
			throw new RecordSetException("fails to run script on the record: " + input + ", cause=" + e);
		}
	}
	
	@Override
	public String toString() {
		String expr = m_script.getScript();
		if ( expr.length() > 50 ) {
			expr = expr.substring(0, 50) + "...";
		}
		
		return String.format("update['%s']", expr);
	}

	public static Update fromProto(UpdateProto proto) {
		RecordScript expr = RecordScript.fromProto(proto.getScript());
		return new Update(expr);
	}

	@Override
	public UpdateProto toProto() {
		return UpdateProto.newBuilder()
						.setScript(m_script.toProto())
						.build();
	}
}
