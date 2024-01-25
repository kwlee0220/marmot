package marmot.optor.reducer;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.io.MultiColumnKey;
import marmot.optor.rset.InMemoryKeyedRecordSetFactory;
import marmot.optor.rset.KeyedGroupTransformedRecordSet;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.optor.support.KeyedRecordSetFactory;
import marmot.proto.optor.ReduceIntermByGroupAtMapSideProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ReduceIntermByGroupAtMapSide extends AbstractRecordSetFunction
								implements PBSerializable<ReduceIntermByGroupAtMapSideProto> {
	private final MultiColumnKey m_keyCols;
	private final MultiColumnKey m_tagCols;
	private final MultiColumnKey m_orderCols;
	private final RecordSetReducer m_reducer;
	
	public ReduceIntermByGroupAtMapSide(MultiColumnKey keyCols, MultiColumnKey tagCols,
											MultiColumnKey orderCols, RecordSetReducer reducer) {
		m_keyCols = keyCols;
		m_tagCols = tagCols;
		m_orderCols = orderCols;
		
		m_reducer = reducer;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		m_marmot = marmot;

		MultiColumnKey grpKeyCols = MultiColumnKey.concat(m_keyCols, m_tagCols);
		RecordSchema keySchema = inputSchema.project(grpKeyCols.getColumnNames());
		RecordSchema valueSchema = inputSchema.complement(grpKeyCols.getColumnNames());
		m_reducer.initialize(marmot, valueSchema);
		
		RecordSchema reducedSchema = m_reducer.getRecordSchema();
		RecordSchema outSchema = RecordSchema.concat(keySchema, reducedSchema);
		
		setInitialized(marmot, inputSchema, outSchema);
	}

	@Override
	public RecordSet apply(RecordSet input) {
		checkInitialized();
		
		KeyedRecordSetFactory groups = new InMemoryKeyedRecordSetFactory(input, m_keyCols, m_tagCols,
															m_orderCols, m_orderCols.length() > 0);
		return new KeyedGroupTransformedRecordSet(toString(), groups, m_reducer);
	}


	public static ReduceIntermByGroupAtMapSide fromProto(ReduceIntermByGroupAtMapSideProto proto) {
		MultiColumnKey cmpKeyCols = MultiColumnKey.fromString(proto.getKeyColumns());
		MultiColumnKey taggedKeyCols = MultiColumnKey.fromString(proto.getTagColumns());
		MultiColumnKey orderKeyCols = MultiColumnKey.fromString(proto.getOrderColumns());
		RecordSetReducer combiner = (RecordSetReducer)PBUtils.deserialize(proto.getReducer());
		
		return new ReduceIntermByGroupAtMapSide(cmpKeyCols, taggedKeyCols, orderKeyCols, combiner);
	}
	
	@Override
	public ReduceIntermByGroupAtMapSideProto toProto() {
		return ReduceIntermByGroupAtMapSideProto.newBuilder()
									.setKeyColumns(m_keyCols.toString())
									.setTagColumns(m_tagCols.toString())
									.setOrderColumns(m_orderCols.toString())
									.setReducer(((PBSerializable<?>)m_reducer).serialize())
									.build();
	}
}
