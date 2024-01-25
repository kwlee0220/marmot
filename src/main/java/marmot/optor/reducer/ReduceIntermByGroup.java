package marmot.optor.reducer;

import org.apache.hadoop.mapreduce.ReduceContext;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.io.MultiColumnKey;
import marmot.mapreduce.ReduceContextKeyedRecordSetFactory;
import marmot.mapreduce.ReduceContextRecordSet;
import marmot.optor.rset.InMemoryKeyedRecordSetFactory;
import marmot.optor.rset.KeyedGroupTransformedRecordSet;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.optor.support.KeyedRecordSetFactory;
import marmot.proto.optor.CombineByGroupProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ReduceIntermByGroup extends AbstractRecordSetFunction
									implements PBSerializable<CombineByGroupProto> {
	private final MultiColumnKey m_keyCols;
	private final MultiColumnKey m_tagCols;
	private final MultiColumnKey m_grpKeyCols;
	private final MultiColumnKey m_orderCols;
	private final RecordSetReducer m_reducer;
	
	public ReduceIntermByGroup(MultiColumnKey keyCols, MultiColumnKey tagCols,
								MultiColumnKey orderCols, RecordSetReducer reducer) {
		m_keyCols = keyCols;
		m_tagCols = tagCols;
		m_grpKeyCols = MultiColumnKey.concat(keyCols, tagCols);
		m_orderCols = orderCols;
		
		m_reducer = reducer;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		m_marmot = marmot;
		
		RecordSchema keySchema = inputSchema.project(m_grpKeyCols.getColumnNames());
		RecordSchema valueSchema = inputSchema.complement(m_grpKeyCols.getColumnNames());
		m_reducer.initialize(marmot, valueSchema);
		RecordSchema reducedSchema = m_reducer.getRecordSchema();
		RecordSchema outSchema = RecordSchema.concat(keySchema, reducedSchema);
		
		setInitialized(marmot, inputSchema, outSchema);
	}

	@Override
	public boolean isStreamable() {
		return false;
	}

	@Override
	public RecordSet apply(RecordSet input) {
		checkInitialized();
		
		KeyedRecordSetFactory groups;
		if ( input instanceof ReduceContextRecordSet ) {
			@SuppressWarnings("rawtypes")
			ReduceContext ctxt = ((ReduceContextRecordSet)input).getReduceContext();
			groups = new ReduceContextKeyedRecordSetFactory(ctxt, input.getRecordSchema(),
															m_keyCols, m_tagCols);
		}
		else {
			groups = new InMemoryKeyedRecordSetFactory(input, m_keyCols, m_tagCols,
														m_orderCols, true);
		}
		
		return new KeyedGroupTransformedRecordSet(toString(), groups, m_reducer);
	}

	public static ReduceIntermByGroup fromProto(CombineByGroupProto proto) {
		MultiColumnKey cmpKeyCols = MultiColumnKey.fromString(proto.getKeyColumns());
		MultiColumnKey taggedKeyCols = MultiColumnKey.fromString(proto.getTagColumns());
		MultiColumnKey orderKeyCols = MultiColumnKey.fromString(proto.getOrderColumns());
		RecordSetReducer combiner = (RecordSetReducer)PBUtils.deserialize(proto.getReducer());
		
		return new ReduceIntermByGroup(cmpKeyCols, taggedKeyCols, orderKeyCols, combiner);
	}
	
	@Override
	public CombineByGroupProto toProto() {
		return CombineByGroupProto.newBuilder()
									.setKeyColumns(m_keyCols.toString())
									.setTagColumns(m_tagCols.toString())
									.setOrderColumns(m_orderCols.toString())
									.setReducer(((PBSerializable<?>)m_reducer).serialize())
									.build();
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		
		builder.append(String.format("keys={%s}", m_keyCols));
		
		if ( m_tagCols.length() > 0 ) {
			builder.append(String.format(", tags={%s}", m_tagCols));
		}
		if ( m_orderCols.length() > 0 ) {
			builder.append(String.format(", order_by={%s}", m_orderCols));
		}
		
		return String.format("reduce_interm_by_group[%s]", builder);
	}
}
