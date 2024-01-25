package marmot.optor.reducer;

import java.util.Map;

import com.google.common.collect.Maps;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordScript;
import marmot.proto.optor.ScriptRecordSetCombinerProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;
import marmot.support.RecordScriptExecution;
import utils.Utilities;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ScriptIntermediateCombiner extends AbstractRecordSetReducer
										implements PBSerializable<ScriptRecordSetCombinerProto> {
	private final RecordScript m_combineScript;
	private final RecordScriptExecution m_combine;
	private final Map<String,Object> m_variables = Maps.newHashMap();
	
	ScriptIntermediateCombiner(FOption<String> initializeScript, String combineScript) {
		Utilities.checkNotNullArgument(initializeScript, "initializeScript is null");
		Utilities.checkNotNullArgument(combineScript, "combineScript is null");
		
		m_combineScript = initializeScript.map(init -> RecordScript.of(init, combineScript))
										.getOrElse(() -> RecordScript.of(combineScript));
		m_combine = RecordScriptExecution.of(m_combineScript);
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		m_combine.initialize(m_variables);
		setInitialized(marmot, inputSchema, inputSchema);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void reduce(Record reduced, Record record, int index) {
		m_variables.put("combined", reduced.toMap());
		m_variables.put("input", record.toMap());
		
		m_combine.execute(m_variables);
		reduced.set((Map)m_variables.get("combined"));
	}

	public static ScriptIntermediateCombiner fromProto(ScriptRecordSetCombinerProto proto) {
		String combineExpr = proto.getCombinerExpr();
		FOption<String> initializeExpr = PBUtils.getOptionField(proto, "initialize_expr");
		
		return new ScriptIntermediateCombiner(initializeExpr, combineExpr);
	}
	
	@Override
	public ScriptRecordSetCombinerProto toProto() {
		ScriptRecordSetCombinerProto.Builder builder
								= ScriptRecordSetCombinerProto.newBuilder()
											.setCombinerExpr(m_combineScript.getScript());
		m_combineScript.getInitializer().ifPresent(builder::setInitializeExpr);
		return builder.build();
	}
}