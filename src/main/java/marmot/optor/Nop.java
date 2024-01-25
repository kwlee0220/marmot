package marmot.optor;

import org.slf4j.LoggerFactory;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.proto.optor.NopProto;
import marmot.support.PBSerializable;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Nop extends AbstractRecordSetFunction
					implements NonParallelizable, PBSerializable<NopProto> {
	public static final Nop get() {
		return new Nop();
	}
	
	/**
	 * Drop 연산자 객체를 생성한다.
	 * Drop 연산은 입력 레코드 세트에 포함된 레코드들 중에서 주어진 갯수의 처음 레코드들을
	 * 제외하는 작업을 수행한다.
	 * 
	 * @param count	제외시킬 처음 레코드의 갯수.
	 */
	private Nop() {
		setLogger(LoggerFactory.getLogger(Nop.class));
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		setInitialized(marmot, inputSchema, inputSchema);
	}

	@Override
	public RecordSet apply(RecordSet input) {
		checkInitialized();
		
		return input;
	}
	
	@Override
	public String toString() {
		return "nop";
	}

	public static Nop fromProto(NopProto proto) {
		return new Nop();
	}

	@Override
	public NopProto toProto() {
		return NopProto.newBuilder().build();
	}
}