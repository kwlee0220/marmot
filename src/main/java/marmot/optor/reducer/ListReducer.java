package marmot.optor.reducer;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.proto.optor.ListReducerProto;
import marmot.support.PBSerializable;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ListReducer extends AbstractRecordSetFunction
						implements RecordSetReducer, PBSerializable<ListReducerProto> {
	private static final ListReducer s_singleton = new ListReducer();
	private static final ListReducerProto s_proto = ListReducerProto.newBuilder().build();
	
	public static final ListReducer get() {
		return s_singleton;
	}
	
	public static final ListReducerProto getProto() {
		return s_proto;
	}
	
	private ListReducer() {}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		setInitialized(marmot, inputSchema, inputSchema);
	}

	@Override
	public RecordSet apply(RecordSet input) {
		return input;
	}

	public static ListReducer fromProto(ListReducerProto proto) {
		return get();
	}
	
	@Override
	public ListReducerProto toProto() {
		return getProto();
	}
}
