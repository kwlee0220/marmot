package marmot.optor.reducer;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordTransform;
import marmot.io.MultiColumnKey;
import marmot.optor.support.RecordLevelTransform;
import marmot.proto.optor.FinalizeByGroupProto;
import marmot.protobuf.PBUtils;
import marmot.support.DefaultRecord;
import marmot.support.PBSerializable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FinalizeIntermByGroup extends RecordLevelTransform
									implements PBSerializable<FinalizeByGroupProto> {
	private final MultiColumnKey m_keyCols;
	private final MultiColumnKey m_tagCols;
	private final MultiColumnKey m_grpKeyCols;
	private final RecordTransform m_finalizer;
	
	private Record m_valueRecord;
	private Record m_finalRecord;
	
	public FinalizeIntermByGroup(MultiColumnKey keyCols, MultiColumnKey tagCols,
									RecordTransform finalizer) {
		m_keyCols = keyCols;
		m_tagCols = tagCols;
		// group-key 와 tag-key을 합쳐서 내부적으로는 group-key로 간주한다.
		m_grpKeyCols = MultiColumnKey.concat(m_keyCols, m_tagCols);
		m_finalizer = finalizer;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		RecordSchema grpKeySchema = inputSchema.project(m_grpKeyCols.getColumnNames());
		RecordSchema valueSchema = inputSchema.complement(m_grpKeyCols.getColumnNames());
		m_finalizer.initialize(marmot, valueSchema);
		m_valueRecord = DefaultRecord.of(valueSchema);

		RecordSchema finalValueSchema = m_finalizer.getRecordSchema();
		RecordSchema outSchema = RecordSchema.concat(grpKeySchema, finalValueSchema);
		m_finalRecord = DefaultRecord.of(finalValueSchema);
		
		setInitialized(marmot, inputSchema, outSchema);
	}

	@Override
	public boolean transform(Record input, Record output) {
		for ( int i =0; i < m_valueRecord.getColumnCount(); ++i ) {
			m_valueRecord.set(i, input.get(i + m_grpKeyCols.length()));
		}
		m_finalizer.transform(m_valueRecord, m_finalRecord);
		
		for ( int i =0; i < m_grpKeyCols.length(); ++i ) {
			output.set(i, input.get(i));
		}
		output.setAll(m_grpKeyCols.length(), m_finalRecord.getAll());
		
		return true;
	}

	@Override
	public String toString() {
		return String.format("finalize_interm_by_group[%s:(%s)]",
							m_keyCols, m_tagCols);
	}

	public static FinalizeIntermByGroup fromProto(FinalizeByGroupProto proto) {
		MultiColumnKey keyCols = MultiColumnKey.fromString(proto.getKeyColumns());
		MultiColumnKey tagCols = MultiColumnKey.fromString(proto.getTagColumns());
		RecordTransform finalizer = (RecordTransform)PBUtils.deserialize(proto.getFinalizer());
		
		return new FinalizeIntermByGroup(keyCols, tagCols, finalizer);
	}
	
	@Override
	public FinalizeByGroupProto toProto() {
		return FinalizeByGroupProto.newBuilder()
								.setKeyColumns(m_keyCols.toString())
								.setTagColumns(m_tagCols.toString())
								.setFinalizer(((PBSerializable<?>)m_finalizer).serialize())
								.build();
	}
}
