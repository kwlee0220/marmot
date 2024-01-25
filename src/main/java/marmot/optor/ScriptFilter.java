package marmot.optor;

import java.util.Map;

import org.slf4j.LoggerFactory;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordScript;
import marmot.optor.support.ColumnVariableResolverFactory;
import marmot.optor.support.Filter;
import marmot.plan.PredicateOptions;
import marmot.proto.optor.ScriptFilterProto;
import marmot.support.DataUtils;
import marmot.support.PBSerializable;
import marmot.support.RecordScriptExecution;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ScriptFilter extends Filter<ScriptFilter>
							implements PBSerializable<ScriptFilterProto> {
	private final RecordScriptExecution m_filterExec;
	private final RecordScript m_script;
	private ColumnVariableResolverFactory m_vrFact;

	public ScriptFilter(RecordScript script) {
		super(PredicateOptions.DEFAULT);
		
		Utilities.checkNotNullArgument(script, "predicate is null");
		
		m_script = script;
		m_filterExec = RecordScriptExecution.of(script);
		
		setLogger(LoggerFactory.getLogger(ScriptFilter.class));
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		Utilities.checkNotNullArgument(marmot, "MarmotServer is null");
		Utilities.checkNotNullArgument(inputSchema,
									() -> String.format("op: %s: input RecordSchema is null", this));
		
		Map<String,Object> args = m_filterExec.getArgumentAll();
		m_vrFact = new ColumnVariableResolverFactory(inputSchema, args).readOnly(true);
		
		m_filterExec.initialize(m_vrFact);
		
		super.initialize(marmot, inputSchema);
	}

	@Override
	public boolean test(Record record) {
		m_vrFact.bind(record);
		try {
			return DataUtils.asBoolean(m_filterExec.execute(m_vrFact));
		}
		catch ( Throwable e ) {
			if ( getLogger().isDebugEnabled() ) {
				String msg = String.format("fails to evaluate the predicate: '%s, record=%s",
											m_filterExec, record);
				getLogger().warn(msg);
			}
			return false;
		}
	}
	
	@Override
	public String toString() {
		return String.format("%s: expr='%s'", getClass().getSimpleName(), m_filterExec);
	}

	public static ScriptFilter fromProto(ScriptFilterProto proto) {
		RecordScript predicate = RecordScript.fromProto(proto.getPredicate());
		return new ScriptFilter(predicate);
	}

	@Override
	public ScriptFilterProto toProto() {
		return ScriptFilterProto.newBuilder()
								.setPredicate(m_script.toProto())
								.build();
	}
}