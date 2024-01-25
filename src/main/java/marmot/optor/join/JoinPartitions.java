package marmot.optor.join;

import org.apache.hadoop.mapreduce.ReduceContext;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.io.MultiColumnKey;
import marmot.mapreduce.ReduceContextKeyedRecordSetFactory;
import marmot.mapreduce.ReduceContextRecordSet;
import marmot.optor.JoinType;
import marmot.optor.KeyColumn;
import marmot.optor.RecordSetOperatorException;
import marmot.optor.rset.InMemoryKeyedRecordSetFactory;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.optor.support.KeyedRecordSetFactory;
import marmot.optor.support.colexpr.ColumnSelector;
import marmot.optor.support.colexpr.ColumnSelectorFactory;
import marmot.proto.optor.JoinPartitionsProto;
import marmot.support.PBSerializable;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JoinPartitions extends AbstractRecordSetFunction
							implements PBSerializable<JoinPartitionsProto> {
	
	private final MultiColumnKey m_joinKey;
	private final String m_leftPrefix;
	private final RecordSchema m_leftSchema;
	private final String m_rightPrefix;
	private final RecordSchema m_rightSchema;
	private final JoinType m_joinType;
	private final String m_outputColumns;
	
	private ColumnSelector m_selector;
	
	public JoinPartitions(MultiColumnKey joinCols, String leftPrefix, RecordSchema leftSchema,
							String rightPrefix, RecordSchema rightSchema, String outputColumns,
							JoinType joinType) {
		m_leftPrefix = leftPrefix;
		m_rightPrefix = rightPrefix;
		m_joinKey = joinCols;
		m_leftSchema = leftSchema;
		m_rightSchema = rightSchema;
		m_outputColumns = outputColumns;
		m_joinType = joinType;
	}
	
	public JoinType getJoinType() {
		return m_joinType;
	}
	
	public MultiColumnKey getJoinKey() {
		return m_joinKey;
	}
	
	public String getLeftNamespace() {
		return m_leftPrefix;
	}
	
	public String getRightNamespace() {
		return m_rightPrefix;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		m_marmot = marmot;
		
		try {
			ColumnSelectorFactory fact = new ColumnSelectorFactory(m_outputColumns);
			fact.addRecordSchema(m_leftPrefix, m_leftSchema);
			fact.addRecordSchema(m_rightPrefix, m_rightSchema);
			m_selector = fact.create();
			
			RecordSchema outSchema = (m_joinType == JoinType.SEMI_JOIN)
										? inputSchema : m_selector.getRecordSchema();
			setInitialized(marmot, inputSchema, outSchema);
		}
		catch ( Exception e ) {
 			throw new RecordSetOperatorException(String.format("op=%s, cause=%s", this, e));
		}
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
															m_joinKey, MultiColumnKey.EMPTY);
		}
		else {
			MultiColumnKey orderCols = TagLoadedJoinKeyColumns.ORDER_KEY; 
			groups = new InMemoryKeyedRecordSetFactory(input, m_joinKey,
														MultiColumnKey.EMPTY, orderCols, false);
		}
						
		return new JoinedRecordSet(this, groups, m_leftSchema, m_rightSchema, m_selector);
	}
	
	@Override
	public String toString() {
		String cmpKeyColStr = m_joinKey.streamKeyColumns()
										.map(KeyColumn::name)
										.join(",");
		
		return String.format("%s[{%s} -> %s]", m_joinType.toString().toLowerCase(),
							cmpKeyColStr, m_outputColumns);
	}

	public static JoinPartitions fromProto(JoinPartitionsProto proto) {
		return new JoinPartitions(
								MultiColumnKey.fromString(proto.getJoinColumns()),
								proto.getLeftPrefix(),
								RecordSchema.fromProto(proto.getLeftRecordSchema()),
								proto.getRightPrefix(),
								RecordSchema.fromProto(proto.getRightRecordSchema()),
								proto.getOutputColumnExpr(),
								JoinType.fromProto(proto.getJoinType()));
	}

	@Override
	public JoinPartitionsProto toProto() {
		return JoinPartitionsProto.newBuilder()
									.setJoinColumns(m_joinKey.toString())
									.setLeftPrefix(m_leftPrefix)
									.setLeftRecordSchema(m_leftSchema.toProto())
									.setRightPrefix(m_rightPrefix)
									.setRightRecordSchema(m_rightSchema.toProto())
									.setOutputColumnExpr(m_outputColumns)
									.setJoinType(m_joinType.toProto())
									.build();
	}
}
