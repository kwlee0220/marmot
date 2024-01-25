package marmot.optor.reducer;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.proto.optor.PutSideBySideProto;
import marmot.support.PBSerializable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PutSideBySide extends AbstractRecordSetReducer
						implements RecordSetReducer, PBSerializable<PutSideBySideProto> {
	private final String m_valueColName;
	private final String m_tagColName;
	private final RecordSchema m_mergedSchema;
	
	private int m_valueColIdx;
	private int m_tagColIdx;
	
	public PutSideBySide(String valColName, String tagColName, RecordSchema mergedSchema) {
		m_valueColName = valColName;
		m_tagColName = tagColName;
		m_mergedSchema = mergedSchema;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		m_valueColIdx = inputSchema.getColumn(m_valueColName).ordinal();
		m_tagColIdx = inputSchema.getColumn(m_tagColName).ordinal();
		
		setInitialized(marmot, inputSchema, m_mergedSchema);
	}

	@Override
	protected void reduce(Record accum, Record record, int index) {
		Object src = record.get(m_valueColIdx);
		if ( src == null ) {
			return;
		}
		
		Object tag = record.get(m_tagColIdx);
		if ( tag == null ) {
			return;
		}
		
		Column outCol = accum.getRecordSchema()
								.findColumn(tag.toString())
								.getOrNull();
		if ( outCol == null ) {
			return;
		}
		
		accum.set(outCol.ordinal(), src);
	}

	public static PutSideBySide fromProto(PutSideBySideProto proto) {
		RecordSchema outputSchema = RecordSchema.fromProto(proto.getOutputSchema());
		return new PutSideBySide(proto.getValueColumn(), proto.getTagColumn(), outputSchema);
	}
	
	@Override
	public PutSideBySideProto toProto() {
		return PutSideBySideProto.newBuilder()
								.setValueColumn(m_valueColName)
								.setTagColumn(m_tagColName)
								.setOutputSchema(m_mergedSchema.toProto())
								.build();
	}
}
