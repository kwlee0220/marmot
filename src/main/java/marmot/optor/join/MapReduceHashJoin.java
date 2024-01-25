package marmot.optor.join;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.MarmotCore;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.dataset.DataSet;
import marmot.optor.CompositeRecordSetFunction;
import marmot.optor.JoinOptions;
import marmot.optor.RecordSetOperatorException;
import marmot.optor.support.JoinUtils;
import marmot.optor.support.colexpr.ColumnSelector;
import marmot.plan.Group;
import marmot.proto.optor.MapReduceEquiJoinProto;
import marmot.support.PBSerializable;
import marmot.support.RecordSetOperatorChain;
import marmot.type.DataType;
import utils.CSV;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MapReduceHashJoin extends CompositeRecordSetFunction
								implements PBSerializable<MapReduceEquiJoinProto> {
	final static Logger s_logger = LoggerFactory.getLogger(MapReduceHashJoin.class);

	public static final RecordSchema SCHEMA_PREFIX = RecordSchema
										.builder()
											.addColumn("dataset_idx", DataType.BYTE)
											.addColumn("cacheable", DataType.BYTE)
											.addColumn("encoded", DataType.BINARY)
										.build();
	public static final int TAG_IDX_DATASET_IDX = 0;
	public static final int TAG_IDX_CACHEABLE = 1;
	public static final int TAG_IDX_DECODED = 2;
	
	private final String m_inputJoinColsExpr;
	private final String m_paramJoinColsExpr;
	private final String m_paramDsId;
	private final String m_outputColExpr;
	private final JoinOptions m_options;
	
	private DataSet m_paramDs = null;
	private ColumnSelector m_selector = null;
	
	public MapReduceHashJoin(String inputJoinColsExpr, String paramDsId,
								String paramJoinColsExpr, String outputColumns,
								JoinOptions opts) {
		Utilities.checkNotNullArgument(inputJoinColsExpr, "input join columns expression");
		Utilities.checkNotNullArgument(paramJoinColsExpr, "parameter join columns expression");
		Utilities.checkNotNullArgument(paramDsId,  "parameter dataset is null");
		Utilities.checkNotNullArgument(outputColumns,  "output columns is null");
		Utilities.checkNotNullArgument(opts,  "JoinOptions is null");
		
		String[] inCols = CSV.parseCsvAsArray(inputJoinColsExpr);
		String[] paramCols = CSV.parseCsvAsArray(paramJoinColsExpr);
		if ( inCols.length != paramCols.length ) {
			throw new IllegalArgumentException("input/parameter join columns do not match: "
												+ "input=" + inputJoinColsExpr
												+ ", param=" + paramJoinColsExpr);
		}
		
		m_inputJoinColsExpr = inputJoinColsExpr;
		m_paramJoinColsExpr = paramJoinColsExpr;
		m_paramDsId = paramDsId;
		m_outputColExpr = outputColumns;
		m_options = opts;
	}

	@Override
	public boolean isMapReduceRequired() {
		return true;
	}
	
	public String getJoinKeyColumns() {
		return m_inputJoinColsExpr;
	}

	@Override
	protected RecordSchema _initialize(MarmotCore marmot, RecordSchema inputSchema) {
		m_marmot = (MarmotCore)marmot;
		m_paramDs = marmot.getDataSet(m_paramDsId);
		
		try {
			m_selector = JoinUtils.createJoinColumnSelector(inputSchema, m_paramDs.getRecordSchema(),
															m_outputColExpr);
			return m_selector.getRecordSchema();
		}
		catch ( Exception e ) {
 			throw new RecordSetOperatorException(String.format("op=%s, cause=%s", this, e));
		}
	}

	@Override
	protected RecordSetOperatorChain createComponents() {
		TagJoinKeyColumns tag = new TagJoinKeyColumns(m_inputJoinColsExpr, m_paramDsId,
														m_paramJoinColsExpr);
		
		JoinPartitionPair joinPartition = new JoinPartitionPair("left", getInputRecordSchema(),
															"right", m_paramDs.getRecordSchema(),
															m_outputColExpr, m_options.joinType());
		
		List<String> inJoinCols = CSV.parseCsv(m_inputJoinColsExpr).toList();
		String joinColsExpr = IntStream.range(0, inJoinCols.size())
										.mapToObj(idx -> String.format("jc%02d", idx))
										.collect(Collectors.joining(","));
		int workerCount = m_options.workerCount().getOrElse(m_marmot.getDefaultPartitionCount());
		
		Plan plan;
		plan = Plan.builder("compose")
						.add(tag)
						.reduceByGroup(Group.ofKeys(joinColsExpr)
											.orderBy("cacheable:D")
											.workerCount(workerCount),
										joinPartition)
						.project(String.format("*-{%s}", joinColsExpr))
						.build();
		
		return RecordSetOperatorChain.from(m_marmot, plan);
	}
	
	@Override
	public String toString() {
		return String.format("%s[{%s}x%s{%s}]", getClass().getSimpleName(),
							m_inputJoinColsExpr, m_paramDsId, m_paramJoinColsExpr);
	}

	public static MapReduceHashJoin fromProto(MapReduceEquiJoinProto proto) {
		return new MapReduceHashJoin(proto.getJoinColumns(),
									proto.getParamDataset(), proto.getParamColumns(),
									proto.getOutputColumnExpr(),
									JoinOptions.fromProto(proto.getJoinOptions()));
	}

	@Override
	public MapReduceEquiJoinProto toProto() {
		return MapReduceEquiJoinProto.newBuilder()
									.setJoinColumns(m_inputJoinColsExpr)
									.setParamDataset(m_paramDsId)
									.setParamColumns(m_paramJoinColsExpr)
									.setOutputColumnExpr(m_outputColExpr)
									.setJoinOptions(m_options.toProto())
									.build();
	}
}
