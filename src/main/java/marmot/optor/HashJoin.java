package marmot.optor;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSetException;
import marmot.dataset.DataSet;
import marmot.optor.join.InnerJoinAtMapSide;
import marmot.optor.join.LeftOuterJoinAtMapSide;
import marmot.optor.join.MapReduceHashJoin;
import marmot.optor.join.SemiJoinAtMapSide;
import marmot.optor.support.JoinUtils;
import marmot.proto.optor.HashJoinProto;
import marmot.support.PBSerializable;
import marmot.support.RecordSetOperatorChain;
import marmot.type.DataType;
import utils.CSV;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class HashJoin extends CompositeRecordSetFunction
						implements PBSerializable<HashJoinProto> {
	private final String m_inputJoinCols;
	private final String m_paramJoinCols;
	private final String m_paramDsId;
	private final String m_outputColumnsExpr;
	private final JoinOptions m_opts;
	
	private HashJoin(String inputJoinCols, String paramDsId,
					String paramJoinCols, String outputColumns, JoinOptions opts) {
		Utilities.checkNotNullArgument(inputJoinCols, "input join columns");
		Utilities.checkNotNullArgument(paramJoinCols, "parameter join columns");
		Utilities.checkNotNullArgument(paramDsId, "parameter dataset id");
		Utilities.checkNotNullArgument(outputColumns, "output columns id");
		Utilities.checkNotNullArgument(opts, "JoinOptions");
		
		m_inputJoinCols = inputJoinCols;
		m_paramJoinCols = paramJoinCols;
		m_paramDsId = paramDsId;
		m_outputColumnsExpr = outputColumns;
		m_opts = opts;
	}
	
	public String getInputKeyColumns() {
		return m_inputJoinCols;
	}
	
	public String getParamDataSetId() {
		return m_paramDsId;
	}
	
	public String getParamKeyColumns() {
		return m_paramJoinCols;
	}
	
	public String getOutputColumnsExpr() {
		return m_outputColumnsExpr;
	}
	
	public JoinOptions getJoinOptions() {
		return m_opts;
	}

	@Override
	public boolean isStreamable() {
		return false;
	}

	@Override
	public boolean isMapReduceRequired() {
		MarmotCore marmot = (MarmotCore)getMarmotCore();
		
		DataSet ds = marmot.getDataSet(m_paramDsId);
		try {
			return ds.length() > marmot.getDefaultMarmotBlockSize();
		}
		catch ( Exception e ) {
			throw new RecordSetException("fails to get the total size: dataset="
											+ m_paramDsId, e);
		}
	}

	@Override
	protected RecordSchema _initialize(MarmotCore marmot, RecordSchema inputSchema) {
		DataSet paramDs = marmot.getDataSet(m_paramDsId);
		RecordSchema paramSchema = paramDs.getRecordSchema();
		
		String[] inputCols = CSV.parseCsv(m_inputJoinCols).toArray(String.class);
		String[] paramCols = CSV.parseCsv(m_paramJoinCols).toArray(String.class);
		if ( inputCols.length != paramCols.length ) {
			throw new IllegalArgumentException("input/parameter join columns do not match");
		}
		
		for ( int i =0; i < inputCols.length; ++i ) {
			String inputColName = inputCols[i];
			String paramColName = paramCols[i];
			
			DataType inputType = inputSchema.getColumn(inputColName).type();
			DataType paramType = paramSchema.getColumn(paramColName).type();
			if ( !inputType.equals(paramType) ) {
				String msg = String.format("join columns are not compatible: {%s:%s} <-> {%s:%s}",
											inputColName, inputType, paramColName, paramType);
				throw new IllegalArgumentException(msg);
			}
		}
		
		switch ( m_opts.joinType() ) {
			case INNER_JOIN:
			case LEFT_OUTER_JOIN:
			case RIGHT_OUTER_JOIN:
			case FULL_OUTER_JOIN:
				try {
					return JoinUtils.createJoinColumnSelector(inputSchema, paramDs.getRecordSchema(),
														m_outputColumnsExpr).getRecordSchema();
				}
				catch ( Exception e ) {
		 			throw new RecordSetOperatorException(String.format("op=%s, cause=%s", this, e));
				}
			case SEMI_JOIN:
				return inputSchema;
			default:
				throw new AssertionError("unknown join-type: " + m_opts.joinType());
		}
	}

	@Override
	protected RecordSetOperatorChain createComponents() {
		RecordSetOperatorChain components = RecordSetOperatorChain.from(m_marmot, m_inputSchema);
		
		DataSet paramDs = m_marmot.getDataSet(m_paramDsId);
		RecordSchema paramSchema = paramDs.getRecordSchema();
		
		String[] inputCols = CSV.parseCsvAsArray(m_inputJoinCols);
		String[] paramCols = CSV.parseCsvAsArray(m_paramJoinCols);

		for ( int i =0; i < inputCols.length; ++i ) {
			String inputColName = inputCols[i];
			String paramColName = paramCols[i];
			
			DataType inputType = m_inputSchema.getColumn(inputColName).type();
			DataType paramType = paramSchema.getColumn(paramColName).type();
			if ( !inputType.equals(paramType) ) {
				String msg = String.format("join columns are not compatible: {%s:%s} <-> {%s:%s}",
											inputColName, inputType, paramColName, paramType);
				throw new IllegalArgumentException(msg);
			}
		}
		
		long nbytes = paramDs.length();
		if ( nbytes < (long)Math.round(1.5 * m_marmot.getDefaultMarmotBlockSize()) ) {
			RecordSetFunction optor = null;
			switch ( m_opts.joinType() ) {
				case INNER_JOIN:
					optor = new InnerJoinAtMapSide(m_inputJoinCols, m_paramDsId,
													m_paramJoinCols, m_outputColumnsExpr);
					break;
				case SEMI_JOIN:
					optor = new SemiJoinAtMapSide(m_inputJoinCols, m_paramDsId,
													m_paramJoinCols);
					break;
				case LEFT_OUTER_JOIN:
					optor = new LeftOuterJoinAtMapSide(m_inputJoinCols, m_paramDsId,
													m_paramJoinCols, m_outputColumnsExpr);
					break;
				default:
					optor = new MapReduceHashJoin(m_inputJoinCols, m_paramDsId,
													m_paramJoinCols, m_outputColumnsExpr,
													m_opts);
			}
			
			components.add(optor);
		}
		else {
			MapReduceHashJoin join = new MapReduceHashJoin(m_inputJoinCols, m_paramDsId,
															m_paramJoinCols, m_outputColumnsExpr,
															m_opts);
			components.add(join);
		}
		
		return components;
	}
	
	@Override
	public String toString() {
		String title = (m_opts.joinType() == JoinType.INNER_JOIN)
						? "join" : m_opts.joinType().name();
		title = title.toLowerCase();
		
		String output = "";
		if ( m_opts.joinType() != JoinType.SEMI_JOIN ) {
			output = ": {" + m_outputColumnsExpr + "}";
		}
		
		return String.format("%s[{%s}x%s{%s}]%s", title, m_inputJoinCols,
											m_paramDsId, m_paramJoinCols, output);
	}

	public static HashJoin fromProto(HashJoinProto proto) {
		return HashJoin.builder()
						.inputJoinColumns(proto.getJoinColumns())
						.paramDataSet(proto.getParamDataset())
						.paramJoinColumns(proto.getParamColumns())
						.outputColumns(proto.getOutputColumnsExpr())
						.options(JoinOptions.fromProto(proto.getJoinOptions()))
						.build();
	}

	@Override
	public HashJoinProto toProto() {
		return HashJoinProto.newBuilder()
						.setJoinColumns(m_inputJoinCols)
						.setParamDataset(m_paramDsId)
						.setParamColumns(m_paramJoinCols)
						.setOutputColumnsExpr(m_outputColumnsExpr)
						.setJoinOptions(m_opts.toProto())
						.build();
	}
	
	public static Builder builder() {
		return new Builder();
	}
	public static class Builder {
		private String m_inputJoinCols;
		private String m_paramJoinCols;
		private String m_paramDsId;
		private String m_outputColumns;
		private JoinOptions m_opts;
		
		public HashJoin build() {
			if ( m_opts == null ) {
				m_opts = JoinOptions.INNER_JOIN;
			}
			
			return new HashJoin(m_inputJoinCols, m_paramDsId, m_paramJoinCols,
								m_outputColumns, m_opts);
		}
		
		public Builder inputJoinColumns(String keyCols) {
			m_inputJoinCols = keyCols;
			return this;
		}
		
		public Builder paramJoinColumns(String keyCols) {
			m_paramJoinCols = keyCols;
			return this;
		}
		
		public Builder paramDataSet(String dsId) {
			m_paramDsId = dsId;
			return this;
		}
		
		public Builder outputColumns(String colExpr) {
			m_outputColumns = colExpr;
			return this;
		}
		
		public Builder options(JoinOptions opts) {
			m_opts = opts;
			return this;
		}
	}
}
