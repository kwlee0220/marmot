package marmot.optor.join;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.dataset.DataSet;
import marmot.io.MultiColumnKey;
import marmot.io.RecordKey;
import marmot.optor.RecordSetOperatorException;
import marmot.optor.rset.SingleInputRecordSet;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.optor.support.JoinUtils;
import marmot.optor.support.colexpr.ColumnSelector;
import marmot.proto.optor.LeftOuterJoinAtMapSideProto;
import marmot.support.DefaultRecord;
import marmot.support.PBSerializable;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LeftOuterJoinAtMapSide extends AbstractRecordSetFunction
									implements PBSerializable<LeftOuterJoinAtMapSideProto> {
	private final String m_inputJoinCols;
	private final String m_paramDsId;
	private final String m_paramJoinCols;
	private final String m_outputColExpr;
	
	private ColumnSelector m_selector;
	
	public LeftOuterJoinAtMapSide(String inputJoinCols, String paramDsId,
									String paramJoinCols, String outputColumns) {
		m_inputJoinCols = inputJoinCols;
		m_paramDsId = paramDsId;
		m_paramJoinCols = paramJoinCols;
		m_outputColExpr = outputColumns;
	}

	@Override
	public boolean isStreamable() {
		return false;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		m_marmot = marmot;
		DataSet info = marmot.getDataSet(m_paramDsId);
		
		try {
			m_selector = JoinUtils.createJoinColumnSelector(inputSchema, info.getRecordSchema(),
															m_outputColExpr);
			
			setInitialized(marmot, inputSchema, m_selector.getRecordSchema());
		}
		catch ( Exception e ) {
 			throw new RecordSetOperatorException(String.format("op=%s, cause=%s", this, e));
		}
	}

	@Override
	public RecordSet apply(RecordSet input) {
		return new Joineds(this, input);
	}
	
	@Override
	public String toString() {
		return String.format("left_outer_join_at_mapside[{%s}x{%s}]",
								m_inputJoinCols, m_paramJoinCols);
	}

	public static LeftOuterJoinAtMapSide fromProto(LeftOuterJoinAtMapSideProto proto) {
		return new LeftOuterJoinAtMapSide(proto.getInputJoinColumns(),
										proto.getParamDataset(), proto.getParamJoinColumns(),
										proto.getOutputColumnExpr());
	}

	@Override
	public LeftOuterJoinAtMapSideProto toProto() {
		return LeftOuterJoinAtMapSideProto.newBuilder()
									.setInputJoinColumns(m_inputJoinCols)
									.setParamDataset(m_paramDsId)
									.setParamJoinColumns(m_paramJoinCols)
									.setOutputColumnExpr(m_outputColExpr)
									.build();
	}
	
	static class Joineds extends SingleInputRecordSet<LeftOuterJoinAtMapSide> {
		private final MultiColumnKey m_inputJoinCols;
		private final MultiColumnKey m_paramJoinCols;
		private final Map<RecordKey,List<Record>> m_lut;
		
		private final Record m_inputRecord;
		private final Record m_emptyParamRecord;
		private Iterator<Record> m_paramMatchIter;
		private Map<String,Record> m_binding = Maps.newHashMap();
		
		Joineds(LeftOuterJoinAtMapSide join, RecordSet input) {
			super(join, input);
			
			m_inputJoinCols = MultiColumnKey.fromString(join.m_inputJoinCols);
			m_paramJoinCols = MultiColumnKey.fromString(join.m_paramJoinCols);
			
			m_inputRecord = newInputRecord();
			
			RecordSet paramRSet = join.m_marmot.getDataSet(join.m_paramDsId).read();
			m_emptyParamRecord = DefaultRecord.of(paramRSet.getRecordSchema());
			m_lut = paramRSet.fstream()
							.groupByKey(r -> RecordKey.from(m_paramJoinCols, r))
							.asMap();
			m_paramMatchIter = Collections.emptyIterator();
		}
		
		@Override
		public boolean next(Record record) {
			checkNotClosed();
			
			while ( true ) {
				if ( m_paramMatchIter.hasNext() ) {
					m_binding.put("right", m_paramMatchIter.next());
					m_optor.m_selector.select(m_binding, record);
					
					return true;
				}
				
				if ( !m_input.next(m_inputRecord) ) {
					return false;
				}
				
				m_binding.put("left", m_inputRecord);
				RecordKey key = RecordKey.from(m_inputJoinCols, m_inputRecord);
				List<Record> matcheds = m_lut.get(key);
				if ( matcheds == null ) {
					m_binding.put("right", m_emptyParamRecord);
					m_optor.m_selector.select(m_binding, record);
					
					return true;
				}
				m_paramMatchIter = matcheds.stream().iterator();
			}
		}
	}
}
