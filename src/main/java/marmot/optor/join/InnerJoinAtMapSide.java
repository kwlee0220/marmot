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
import marmot.optor.RecordSetFunction;
import marmot.optor.RecordSetOperatorException;
import marmot.optor.rset.SingleInputRecordSet;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.optor.support.JoinUtils;
import marmot.optor.support.colexpr.ColumnSelector;
import marmot.proto.optor.InnerJoinAtMapSideProto;
import marmot.support.PBSerializable;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class InnerJoinAtMapSide extends AbstractRecordSetFunction
								implements PBSerializable<InnerJoinAtMapSideProto> {
	private final String m_inputJoinCols;
	private final String m_paramJoinCols;
	private final String m_paramDsId;
	private final String m_outputColumnsExpr;
	
	private volatile ColumnSelector m_selector;
	
	public InnerJoinAtMapSide(String inputJoinCols, String paramDsId,
								String paramJoinCols, String outputColumns) {
		m_inputJoinCols = inputJoinCols;
		m_paramJoinCols = paramJoinCols;
		m_paramDsId = paramDsId;
		m_outputColumnsExpr = outputColumns;
	}

	@Override
	public boolean isStreamable() {
		return false;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		m_marmot = marmot;
		
		DataSet paramInfo = marmot.getDataSet(m_paramDsId);
		
		try {
			m_selector = JoinUtils.createJoinColumnSelector(inputSchema, paramInfo.getRecordSchema(),
															m_outputColumnsExpr);
			
			setInitialized(marmot, inputSchema, m_selector.getRecordSchema());
		}
		catch ( Exception e ) {
 			throw new RecordSetOperatorException(String.format("op=%s, cause=%s", this, e));
		}
	}

	@Override
	public RecordSet apply(RecordSet input) {
		return new JoinedRecordSet(this, input);
	}
	
	@Override
	public String toString() {
		return String.format("inner_join_mapside[{%s}x{%s:%s}]",
							m_inputJoinCols, m_paramDsId, m_paramJoinCols);
	}

	public static InnerJoinAtMapSide fromProto(InnerJoinAtMapSideProto proto) {
		return new InnerJoinAtMapSide(proto.getJoinColumns(),
									proto.getParamDataset(), proto.getParamColumns(),
									proto.getOutputColumnsExpr());
	}

	@Override
	public InnerJoinAtMapSideProto toProto() {
		return InnerJoinAtMapSideProto.newBuilder()
										.setJoinColumns(m_inputJoinCols)
										.setParamDataset(m_paramDsId)
										.setParamColumns(m_paramJoinCols)
										.setOutputColumnsExpr(m_outputColumnsExpr)
										.build();
	}
	
	class JoinedRecordSet extends SingleInputRecordSet<RecordSetFunction> {
		private final MultiColumnKey m_inputJoinKeys;
		private final MultiColumnKey m_paramJoinKeys;
		private Map<RecordKey,List<Record>> m_lut;
		private final Record m_inputRecord;
		private Iterator<Record> m_paramMatchIter;
		private Map<String,Record> m_binding = Maps.newHashMap();
		
		private long m_outerCount = 0;
		private long m_matcheds = 0;
		
		JoinedRecordSet(InnerJoinAtMapSide join, RecordSet input) {
			super(join, input);

			m_inputJoinKeys = MultiColumnKey.fromString(m_inputJoinCols);
			m_paramJoinKeys = MultiColumnKey.fromString(m_paramJoinCols);
			
			m_paramMatchIter = Collections.emptyIterator();
			m_inputRecord = newInputRecord();
		}
		
		@Override
		public boolean next(Record record) {
			checkInitialized();
			
			if ( m_lut == null ) {
				m_lut = populateLookupTable();
			}
			
			while ( true ) {
				if ( m_paramMatchIter.hasNext() ) {
					m_binding.put("right", m_paramMatchIter.next());
					m_selector.select(m_binding, record);
					++m_matcheds;
					
					return true;
				}
				
				if ( !m_input.next(m_inputRecord) ) {
					return false;
				}
				
				m_binding.put("left", m_inputRecord);
				RecordKey key = RecordKey.from(m_inputJoinKeys, m_inputRecord);
				List<Record> matcheds = m_lut.getOrDefault(key, Collections.emptyList());
				m_paramMatchIter = matcheds.stream().iterator();
				
				++m_outerCount;
			}
		}
		
		@Override
		public String toString() {
			return String.format("%s: distinct_count=%d matcheds=%d outer_count=%d",
									m_optor, m_lut.size(), m_matcheds, m_outerCount);
		}
		
		private Map<RecordKey,List<Record>> populateLookupTable() {
			return m_marmot.getDataSet(m_paramDsId)
							.read()
							.fstream()
							.tagKey(r -> RecordKey.from(m_paramJoinKeys, r))
							.groupByKey()
							.asMap();
		}
	}
}
