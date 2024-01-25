package marmot.optor.reducer;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.optor.support.RecordLevelTransform;
import marmot.optor.support.ScriptRecordTransform;
import marmot.proto.optor.ScriptRecordSetReducerProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CombineableScriptRecordSetReducer extends CombineableRecordSetReducer
											implements PBSerializable<ScriptRecordSetReducerProto> {
	private final RecordSchema m_outputSchema;
	private final RecordSchema m_intermSchema;
	private final String m_producerExpr;
	private final FOption<String> m_combinerInitializeExpr;
	private final String m_combinerExpr;
	private final String m_finalizerExpr;
	
	private CombineableScriptRecordSetReducer(
										RecordSchema outputSchema, RecordSchema intermSchema,
										String producerExpr, FOption<String> combinerInitializeExpr,
										String combinerExpr, String finalizerExpr) {
		m_outputSchema = outputSchema;
		m_intermSchema = intermSchema;
		m_producerExpr = producerExpr;
		m_combinerInitializeExpr = combinerInitializeExpr;
		m_combinerExpr = combinerExpr;
		m_finalizerExpr = finalizerExpr;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		setInitialized(marmot, inputSchema, m_outputSchema);
	}

	@Override
	public RecordLevelTransform newIntermediateProducer() {
		return new ScriptRecordTransform(m_intermSchema, m_producerExpr);
	}

	@Override
	public RecordSetReducer newIntermediateReducer() {
		return new ScriptIntermediateCombiner(m_combinerInitializeExpr, m_combinerExpr);
	}

	@Override
	public RecordLevelTransform newIntermediateFinalizer() {
		return new ScriptRecordTransform(m_outputSchema, m_finalizerExpr);
	}
	
	@Override
	public String toString() {
		return String.format("reduce_using_script");
	}

	public static CombineableScriptRecordSetReducer fromProto(ScriptRecordSetReducerProto proto) {
		RecordSchema outputSchema = RecordSchema.fromProto(proto.getOutputSchema());
		RecordSchema intermSchema = RecordSchema.fromProto(proto.getIntermediateSchema());
		String producerExpr = proto.getProducerExpr();
		FOption<String> combinerInitializeExpr = PBUtils.getOptionField(proto,
															"combiner_initialize_expr");
		String combinerExpr = proto.getCombinerExpr();
		String finalizerExpr = proto.getFinalizerExpr();
		
		return new CombineableScriptRecordSetReducer(outputSchema, intermSchema, producerExpr,
									combinerInitializeExpr, combinerExpr, finalizerExpr);
	}
	
	@Override
	public ScriptRecordSetReducerProto toProto() {
		ScriptRecordSetReducerProto.Builder builder
					= ScriptRecordSetReducerProto.newBuilder()
									.setOutputSchema(m_outputSchema.toProto())
									.setIntermediateSchema(m_intermSchema.toProto())
									.setProducerExpr(m_producerExpr)
									.setCombinerExpr(m_combinerExpr)
									.setFinalizerExpr(m_finalizerExpr);
		m_combinerInitializeExpr.ifPresent(builder::setCombinerInitializeExpr);
		return builder.build();
	}
}
