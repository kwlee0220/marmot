package marmot.optor.support;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.proto.optor.IdentityRecordTransformProto;
import marmot.support.PBSerializable;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class IdentityRecordTransform extends RecordLevelTransform
							implements PBSerializable<IdentityRecordTransformProto> {
	private static final IdentityRecordTransform IDENTITY = new IdentityRecordTransform();
	public static final IdentityRecordTransformProto PROTO = IDENTITY.toProto();
	
	public static final IdentityRecordTransform get() {
		return IDENTITY;
	}
	
	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		setInitialized(marmot, inputSchema, inputSchema);
	}

	@Override
	public boolean transform(Record input, Record output) {
		output.set(input);
		
		return true;
	}
	
	public static IdentityRecordTransform fromProto(IdentityRecordTransformProto proto) {
		return new IdentityRecordTransform();
	}

	@Override
	public IdentityRecordTransformProto toProto() {
		return IdentityRecordTransformProto.newBuilder().build();
	}
}