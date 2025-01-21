package marmot.optor.join;

import utils.CSV;
import utils.io.IOUtils;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.io.MultiColumnKey;
import marmot.io.RecordWritable;
import marmot.mapreduce.MarmotMRContext;
import marmot.mapreduce.MarmotMRContextAware;
import marmot.mapreduce.MarmotMRContexts;
import marmot.optor.rset.SingleInputRecordSet;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.proto.optor.TagJoinKeyColumnsProto;
import marmot.support.DefaultRecord;
import marmot.support.PBSerializable;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TagJoinKeyColumns extends AbstractRecordSetFunction
								implements MarmotMRContextAware,
											PBSerializable<TagJoinKeyColumnsProto> {
	private final String m_inputJoinColsExpr;
	private final String m_paramDsId;
	private final String m_paramJoinColsExpr;
	private boolean m_isFirstMapper = true;
	
	TagJoinKeyColumns(String inputJoinColsExpr, String paramDsId, String paramJoinColsExpr) {
		m_inputJoinColsExpr = inputJoinColsExpr;
		m_paramJoinColsExpr = paramJoinColsExpr;
		m_paramDsId = paramDsId;
	}
	
	@Override
	public void setMarmotMRContext(MarmotMRContext context) {
		m_isFirstMapper = context.getTaskOrdinal() == 0;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		RecordSchema.Builder builder = MapReduceHashJoin.SCHEMA_PREFIX.toBuilder();
		
		int idx = 0;
		for ( String name: CSV.parseCsv(m_inputJoinColsExpr, ',', '\\').toList() ) {
			builder.addColumn(String.format("jc%02d", idx),
							inputSchema.getColumn(name).type());
			++idx;
		}
		
		setInitialized(marmot, inputSchema, builder.build());
	}

	@Override
	public RecordSet apply(RecordSet input) {
		checkInitialized();
		
		return new TaggedRecordSet(this, input);
	}
	
	@Override
	public String toString() {
		return String.format("tag_joinkeycols[{%s} x %s{%s}]", m_inputJoinColsExpr,
												m_paramDsId, m_paramJoinColsExpr);
	}

	public static TagJoinKeyColumns fromProto(TagJoinKeyColumnsProto proto) {
		return new TagJoinKeyColumns(proto.getJoinColumns(),
									proto.getParamDataset(), proto.getParamColumns());
	}

	@Override
	public TagJoinKeyColumnsProto toProto() {
		return TagJoinKeyColumnsProto.newBuilder()
									.setJoinColumns(m_inputJoinColsExpr)
									.setParamDataset(m_paramDsId)
									.setParamColumns(m_paramJoinColsExpr)
									.build();
	}

	private static final long CHECKPOINT_INTERVAL = 100000;
	class TaggedRecordSet extends SingleInputRecordSet<TagJoinKeyColumns> {
		private final MultiColumnKey m_inputJoinCols;
		private final MultiColumnKey m_paramJoinCols;
		
		private RecordSet m_paramRSet;	// paramDataSet 적재 여부 결정
		private long m_paramWriteCount;
		
		TaggedRecordSet(TagJoinKeyColumns tag, RecordSet input) {
			super(tag, input);
			
			m_inputJoinCols = MultiColumnKey.fromString(tag.m_inputJoinColsExpr);
			m_paramJoinCols = MultiColumnKey.fromString(tag.m_paramJoinColsExpr);

			// 조인에 참여하는 각 조인 컬럼의 갯수 및 타입이 서로 일치하는지 확인한다.
			RecordSchema inSchema = input.getRecordSchema();
			RecordSchema paramSchema = m_marmot.getDataSet(m_paramDsId)
											.getRecordSchema();
			int njcols = (int)m_inputJoinCols.streamKeyColumns().count();
			if ( m_paramJoinCols.length() != njcols ) {
				String msg = String.format("incompatible join column count: %s<->%s, optor=%s",
										m_inputJoinCols, m_paramJoinCols, TagJoinKeyColumns.this);
				throw new RecordSetException(msg);
			}
			for ( int i =0; i < njcols; ++i ) {
				String inColName = m_inputJoinCols.getKeyColumnAt(i).name();
				Column inCol = inSchema.getColumn(inColName);
				String paramColName = m_paramJoinCols.getKeyColumnAt(i).name();
				Column paramCol = paramSchema.getColumn(paramColName);
				
				if ( !inCol.type().equals(paramCol.type()) ) {
					String msg = String.format("incompatible join column: %s<->%s, optor=%s",
												inCol, paramCol, TagJoinKeyColumns.this);
					throw new RecordSetException(msg);
				}
			}
		}
		
		@Override
		protected void closeInGuard() {
			IOUtils.closeQuietly(m_paramRSet);
			
			super.closeInGuard();
		}
		
		@Override
		public boolean next(Record record) {
			checkNotClosed();
			
			// 'm_paramSet == null' 유무에 따라 input 데이터세트에 대한 처리 여부를 판단함.
			// 'm_paramSet == null'인 경우는 아직 input 레코드에 대한 처리를 진행 중인 것으로 간주.
			if ( m_paramRSet == null ) {
				if ( toOutput(0, m_inputJoinCols, m_input, record) ) {
					return true;
				}
				
				//
				// input 데이터세트에 tagging이 끝난 경우.
				// task-ordinal이 0이 아닌  mapper의 경우는 종료시키고
				// 0번 mapper의 경우는 parameter dataset을 출력시킨다.
				//
				if ( !m_isFirstMapper ) {
					return false;
				}
				
				MapReduceHashJoin.s_logger.info("start pumping parameter dataset: {}",
													m_paramDsId);
				
				// input 데이터 세트를 다 처리한 경우
				// parameter 데이터 세트를 적재하여 처리한다.
				m_paramRSet = m_marmot.getDataSet(m_paramDsId).read();
				m_paramWriteCount = 0;
			}
			
			boolean done = toOutput(1, m_paramJoinCols, m_paramRSet, record);
			if ( done && (++m_paramWriteCount % CHECKPOINT_INTERVAL) == 0 ) {
				MapReduceHashJoin.s_logger.info("pumping parameter dataset: count={}",
												m_paramWriteCount);
				MarmotMRContexts.reportProgress();
			}
			else if ( !done ) {
				MapReduceHashJoin.s_logger.info("finish pumping parameter dataset: {}, count={}",
												m_paramDsId, m_paramWriteCount);
			}
			
			return done;
		}
		
		private boolean toOutput(int dsIdx, MultiColumnKey joinCols, RecordSet rset,
								Record output) {
			RecordSchema schema = rset.getRecordSchema();
			Record input = DefaultRecord.of(schema);
			
			if ( rset.next(input) ) {
				output.set(MapReduceHashJoin.TAG_IDX_DATASET_IDX, (byte)dsIdx);
				output.set(MapReduceHashJoin.TAG_IDX_CACHEABLE, (byte)dsIdx);
				output.set(MapReduceHashJoin.TAG_IDX_DECODED, RecordWritable.from(input).toBytes());
				
				joinCols.streamKeyColumns()
						.map(kc -> input.get(kc.name()))
						.zipWithIndex(3)
						.forEach(t -> output.set(t.index(), t.value()));
				
				return true;
			}
			else {
				return false;
			}
		}
	}
}
