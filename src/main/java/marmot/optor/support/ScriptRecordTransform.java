package marmot.optor.support;

import java.util.Map;

import com.google.common.collect.Maps;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordScript;
import marmot.RecordSetException;
import marmot.proto.optor.ScriptTransformProto;
import marmot.support.DefaultRecord;
import marmot.support.PBSerializable;
import marmot.support.RecordMap;
import marmot.support.RecordScriptExecution;
import utils.Throwables;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ScriptRecordTransform extends RecordLevelTransform
									implements PBSerializable<ScriptTransformProto> {
	private final RecordScript m_transform;
	private final RecordScriptExecution m_transformExec;

	private final Map<String,Object> m_variables = Maps.newHashMap();
	private final Record m_output;
	
	public ScriptRecordTransform(RecordSchema outputSchema, String script) {
		this(outputSchema, RecordScript.of(script));
	}
	
	public ScriptRecordTransform(RecordSchema outputSchema, RecordScript script) {
		Utilities.checkNotNullArgument(outputSchema, "outputSchema is null");
		Utilities.checkNotNullArgument(script, "transform script is null");
		
		m_outputSchema = outputSchema;
		m_transform = script;
		m_transformExec = RecordScriptExecution.of(script);
		m_output = DefaultRecord.of(outputSchema);
		m_variables.put("output", new RecordMap(m_output));
	}
	
	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		Utilities.checkNotNullArgument(marmot, "marmot is null");
		Utilities.checkNotNullArgument(inputSchema, "inputSchema is null");
		
		m_transformExec.initialize(m_variables);
		
		setInitialized(marmot, inputSchema, m_outputSchema);
	}

	@Override
	public boolean transform(Record input, Record output) {
		m_variables.put("input", new RecordMap(input).readonly(true));
		try {
			m_output.clear();
			
			m_transformExec.execute(m_variables);
			output.set(m_output);
			
			return true;
		}
		catch ( Throwable e ) {
			e = Throwables.unwrapThrowable(e);
			Throwables.throwIfInstanceOf(e, RecordSetException.class);
			
			throw new RecordSetException("fails to run script on the record: " + input + ", cause=" + e);
		}
	}
	
	public static ScriptRecordTransform fromProto(ScriptTransformProto proto) {
		RecordSchema outputSchema = RecordSchema.parse(proto.getOutputSchemaExpr());
		RecordScript script = RecordScript.fromProto(proto.getScript());
		return new ScriptRecordTransform(outputSchema, script);
	}

	@Override
	public ScriptTransformProto toProto() {
		return ScriptTransformProto.newBuilder()
									.setOutputSchemaExpr(m_outputSchema.toString())
									.setScript(m_transform.toProto())
									.build();
	}
}