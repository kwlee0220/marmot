package marmot.optor.reducer;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordTransform;
import marmot.io.MultiColumnKey;
import marmot.optor.support.RecordLevelTransform;
import marmot.proto.optor.ProduceByGroupProto;
import marmot.protobuf.PBUtils;
import marmot.support.DefaultRecord;
import marmot.support.PBSerializable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ProduceIntermByGroup extends RecordLevelTransform
									implements PBSerializable<ProduceByGroupProto> {
	private final MultiColumnKey m_keyCols;
	private final MultiColumnKey m_tagCols;
	private final MultiColumnKey m_grpKeyCols;
	private final RecordTransform m_producer;
	
	private int[] m_groupKeyColIdxes;
	private int[] m_valueColIdxes;
	
	private Record m_valueRecord;
	private Record m_reducedValueRecord;
	
	public ProduceIntermByGroup(MultiColumnKey keyCols, MultiColumnKey tagCols,
									RecordTransform producer) {
		m_keyCols = keyCols;
		m_tagCols = tagCols;
		// group-key 와 tag-key을 합쳐서 내부적으로는 group-key로 간주한다.
		m_grpKeyCols = MultiColumnKey.concat(m_keyCols, m_tagCols);
		m_producer = producer;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		RecordSchema grpKeySchema = inputSchema.project(m_grpKeyCols.getColumnNames());
		RecordSchema valueSchema = inputSchema.complement(m_grpKeyCols.getColumnNames());
		m_producer.initialize(marmot, valueSchema);
		RecordSchema reducedValueSchema = m_producer.getRecordSchema();
		RecordSchema outSchema = RecordSchema.concat(grpKeySchema, reducedValueSchema);
		
		m_groupKeyColIdxes = m_grpKeyCols.streamKeyColumns()
										.map(kc -> inputSchema.getColumn(kc.name()))
										.mapToInt(Column::ordinal)
										.toArray();
		m_valueColIdxes = valueSchema.streamColumns()
									.map(Column::name)
									.mapToInt(name -> inputSchema.getColumn(name).ordinal())
									.toArray();
		
		m_valueRecord = DefaultRecord.of(valueSchema);
		m_reducedValueRecord = DefaultRecord.of(reducedValueSchema);
		
		setInitialized(marmot, inputSchema, outSchema);
	}

	public boolean transform(Record input, Record output) {
		for ( int i =0; i < m_groupKeyColIdxes.length; ++i ) {
			output.set(i, input.get(m_groupKeyColIdxes[i]));
		}
		
		for ( int i =0; i < m_valueColIdxes.length; ++i ) {
			m_valueRecord.set(i, input.get(m_valueColIdxes[i]));
		}
		m_producer.transform(m_valueRecord, m_reducedValueRecord);
		for ( int i =0; i < m_reducedValueRecord.getColumnCount(); ++i ) {
			output.set(i+m_groupKeyColIdxes.length, m_reducedValueRecord.get(i));
		}
		
		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		
		builder.append(String.format("keys={%s}", m_keyCols));
		
		if ( m_tagCols.length() > 0 ) {
			builder.append(String.format(", tags={%s}", m_tagCols));
		}
		
		return String.format("%s: %s", getClass().getSimpleName(), builder);
	}

	public static ProduceIntermByGroup fromProto(ProduceByGroupProto proto) {
		MultiColumnKey groupKeyCols = MultiColumnKey.fromString(proto.getKeyColumns());
		MultiColumnKey tagKeyCols = MultiColumnKey.fromString(proto.getTagColumns());
		RecordTransform producer = (RecordTransform)PBUtils.deserialize(proto.getProducer());
		
		return new ProduceIntermByGroup(groupKeyCols, tagKeyCols, producer);
	}
	
	@Override
	public ProduceByGroupProto toProto() {
		return ProduceByGroupProto.newBuilder()
								.setKeyColumns(m_keyCols.toString())
								.setTagColumns(m_tagCols.toString())
								.setProducer(((PBSerializable<?>)m_producer).serialize())
								.build();
	}
}
