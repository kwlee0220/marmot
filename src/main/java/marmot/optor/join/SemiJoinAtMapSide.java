package marmot.optor.join;

import java.util.List;
import java.util.Map;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.io.MultiColumnKey;
import marmot.io.RecordKey;
import marmot.optor.rset.SingleInputRecordSet;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.proto.optor.SemiJoinAtMapSideProto;
import marmot.support.PBSerializable;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SemiJoinAtMapSide extends AbstractRecordSetFunction
								implements PBSerializable<SemiJoinAtMapSideProto> {
	private final String m_inputJoinCols;
	private final String m_paramDsId;
	private final String m_paramJoinCols;
	
	public SemiJoinAtMapSide(String inputJoinCols, String paramDsId,
							String paramJoinCols) {
		m_inputJoinCols = inputJoinCols;
		m_paramDsId = paramDsId;
		m_paramJoinCols = paramJoinCols;
	}

	@Override
	public boolean isStreamable() {
		return false;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		setInitialized(marmot, inputSchema, inputSchema);
	}

	@Override
	public RecordSet apply(RecordSet input) {
		return new JoinedRecordSet(this, input);
	}
	
	@Override
	public String toString() {
		return String.format("semi_join_at_mapside[{%s}x{%s}]",
							m_inputJoinCols, m_paramJoinCols);
	}

	public static SemiJoinAtMapSide fromProto(SemiJoinAtMapSideProto proto) {
		return new SemiJoinAtMapSide(proto.getInputJoinColumns(),
										proto.getParamDataset(),
										proto.getParamJoinColumns());
	}

	@Override
	public SemiJoinAtMapSideProto toProto() {
		return SemiJoinAtMapSideProto.newBuilder()
									.setInputJoinColumns(m_inputJoinCols)
									.setParamDataset(m_paramDsId)
									.setParamJoinColumns(m_paramJoinCols)
									.build();
	}
	
	private static class JoinedRecordSet extends SingleInputRecordSet<SemiJoinAtMapSide> {
		private final MultiColumnKey m_inputJoinCols;
		private final MultiColumnKey m_paramJoinCols;
		private Map<RecordKey,List<Record>> m_lut;
		
		private long m_inputCount;
		private long m_outputCount;
		
		JoinedRecordSet(SemiJoinAtMapSide optor, RecordSet input) {
			super(optor, input);
			
			m_inputJoinCols = MultiColumnKey.fromString(optor.m_inputJoinCols);
			m_paramJoinCols = MultiColumnKey.fromString(optor.m_paramJoinCols);
		}
		
		@Override
		public boolean next(Record record) {
			checkNotClosed();
			
			if ( m_lut == null ) {
				m_lut = buildParamLookupTable();
			}
			
			while ( m_input.next(record) ) {
				++m_inputCount;
				RecordKey key = RecordKey.from(m_inputJoinCols, record);
				if ( m_lut.containsKey(key) ) {
					++m_outputCount;
					return true;
				}
			}
			
			return false;
		}
		
		@Override
		public String toString() {
			return String.format("%s: %d->%d", m_optor, m_inputCount, m_outputCount);
		}
		
		private Map<RecordKey,List<Record>> buildParamLookupTable() {
			return m_optor.getMarmotCore()
							.getDataSet(m_optor.m_paramDsId)
							.read()
							.fstream()
							.tagKey(r -> RecordKey.from(m_paramJoinCols, r))
							.groupByKey()
							.asMap();
		}
	}
}
