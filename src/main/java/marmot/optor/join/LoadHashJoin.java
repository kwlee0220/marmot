package marmot.optor.join;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import marmot.Column;
import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.dataset.DataSet;
import marmot.io.MultiColumnKey;
import marmot.mapreduce.MarmotMapOutputKeyColumns;
import marmot.mapreduce.PlanMRExecutionMode;
import marmot.optor.JoinOptions;
import marmot.optor.JoinType;
import marmot.optor.LoadMarmotFile;
import marmot.optor.MapReduceJoint;
import marmot.optor.MapReduceableRecordSetLoader;
import marmot.optor.RecordSetOperatorException;
import marmot.optor.geo.LoadSpatialIndexJoin;
import marmot.optor.support.AbstractRecordSetLoader;
import marmot.optor.support.JoinUtils;
import marmot.proto.optor.LoadHashJoinProto;
import marmot.support.PBSerializable;
import marmot.support.RecordSetOperatorChain;
import marmot.type.DataType;
import utils.CSV;
import utils.Utilities;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LoadHashJoin extends AbstractRecordSetLoader
							implements MapReduceableRecordSetLoader,
										PBSerializable<LoadHashJoinProto> {
	private final String m_leftDsId;
	private final String m_rightDsId;
	private final String m_leftJoinCols;
	private final String m_rightJoinCols;
	private final String m_outputColumnsExpr;
	private final JoinOptions m_opts;
	
	private DataSet m_left;
	private DataSet m_right;
	private MultiColumnKey m_joinKey;
	
	private LoadHashJoin(String leftDataSet, String leftJoinCols,
						String rightDataSet, String rightJoinCols,
						String outputColumns, JoinOptions opts) {
		Utilities.checkNotNullArgument(leftDataSet, "left dataset id is null");
		Utilities.checkNotNullArgument(rightDataSet, "right dataset id is null");
		Utilities.checkNotNullArgument(leftJoinCols, "left join columns");
		Utilities.checkNotNullArgument(rightJoinCols, "right join columns");
		Utilities.checkNotNullArgument(outputColumns, "output columns");
		Utilities.checkNotNullArgument(opts,  "JoinOptions");
		
		m_leftDsId = leftDataSet;
		m_rightDsId = rightDataSet;
		m_leftJoinCols = leftJoinCols;
		m_rightJoinCols = rightJoinCols;
		m_outputColumnsExpr = outputColumns;
		m_opts = opts;
		
		setLogger(LoggerFactory.getLogger(LoadSpatialIndexJoin.class));
	}
	
	public String getOutputColumnsExpr() {
		return m_outputColumnsExpr;
	}
	
	public JoinOptions getJoinOptions() {
		return m_opts;
	}

	@Override
	public void initialize(MarmotCore marmot) {
		m_left = marmot.getDataSet(m_leftDsId);
		RecordSchema leftSchema = m_left.getRecordSchema();
		
		m_right = marmot.getDataSet(m_rightDsId);
		RecordSchema rightSchema = m_right.getRecordSchema();
		
		String[] leftJoinColNames = CSV.parseCsvAsArray(m_leftJoinCols);
		String[] rightJoinColNames = CSV.parseCsvAsArray(m_rightJoinCols);
		if ( leftJoinColNames.length == 0
			|| leftJoinColNames.length != rightJoinColNames.length ) {
			throw new IllegalArgumentException("left/right join columns do not match");
		}
		
		for ( int i =0; i < leftJoinColNames.length; ++i ) {
			String leftColName = leftJoinColNames[i];
			String rightColName = rightJoinColNames[i];
			
			DataType leftType = leftSchema.findColumn(leftColName)
											.map(Column::type)
											.getOrNull();
			if ( leftType == null ) {
				String msg = String.format("invalid left join_column: '%s', schema=%s",
											leftColName, leftSchema);
				throw new IllegalArgumentException(msg);
			}
			
			DataType rightType = rightSchema.findColumn(rightColName)
											.map(Column::type)
											.getOrNull();
			if ( rightType == null ) {
				String msg = String.format("invalid right join_column: '%s', schema=%s",
											rightColName, rightSchema);
				throw new IllegalArgumentException(msg);
			}
			
			if ( !leftType.equals(rightType) ) {
				String msg = String.format("join columns are not compatible: {%s:%s} <-> {%s:%s}",
											leftColName, leftType, rightColName, rightType);
				throw new IllegalArgumentException(msg);
			}
		}
		
		String joinColNames = FStream.range(0, leftJoinColNames.length)
									.mapToObj(idx -> String.format("jc%02d", idx))
									.join(",");
		m_joinKey = MultiColumnKey.fromString(joinColNames); 		
		
		try {
			RecordSchema outSchema = null;
			switch ( m_opts.joinType() ) {
				case INNER_JOIN:
				case LEFT_OUTER_JOIN:
				case RIGHT_OUTER_JOIN:
				case FULL_OUTER_JOIN:
					outSchema = JoinUtils.createJoinColumnSelector(leftSchema, rightSchema,
																	m_outputColumnsExpr)
										.getRecordSchema();
					break;
				case SEMI_JOIN:
					outSchema = leftSchema;
					break;
				default:
					throw new AssertionError("unknown join-type: " + m_opts.joinType());
			}
			
			setInitialized(marmot, outSchema);
		}
		catch ( Exception e ) {
 			throw new RecordSetOperatorException(String.format("op=%s, cause=%s", this, e));
		}
	}

	@Override
	public MapReduceJoint getMapReduceJoint(RecordSchema inputSchema) {
		checkInitialized();
		
		MapReduceJoint joint = MapReduceJoint.create();
			
		joint.setMapOutputKey(MarmotMapOutputKeyColumns.fromGroupKey(m_joinKey,
																TagLoadedJoinKeyColumns.ORDER_KEY));
		joint.setReducerCount(m_opts.workerCount().getOrElse(m_marmot.getDefaultPartitionCount()));
		if ( joint.getReducerCount() > 1 ) {
			// partitioning할 때는 추가된 키 값을 사용하지 않기 위해 별도의 Partitioner를 사용한다.
			joint.setPartitionerClass(EquiJoinPartitioner.class);
		}
		
		// mapper chain 설정
		TagLoadedJoinKeyColumns tag = new TagLoadedJoinKeyColumns(m_leftDsId, m_leftJoinCols,
															m_rightDsId, m_rightJoinCols,
															m_outputColumnsExpr, m_opts);
		tag.initialize(m_marmot);
		RecordSchema intermSchema = tag.getRecordSchema();
		joint.addMapper(tag);
		
		// reducer chain 설정
		RecordSchema leftSchema = m_left.getRecordSchema();
		RecordSchema rightSchema = m_right.getRecordSchema();
								
		JoinPartitions joinPart = new JoinPartitions(m_joinKey, "left", leftSchema,
													"right", rightSchema,
													m_outputColumnsExpr, m_opts.joinType());
		joinPart.initialize(m_marmot, intermSchema);
		joint.addReducer(joinPart);
		
		return joint;
	}

	@Override
	public void configure(Job job) {
		checkInitialized();
	}

	@Override
	public PlanMRExecutionMode getExecutionMode(MarmotCore marmot) throws IOException {
		checkInitialized();
		
		List<Path> leftPaths = Lists.newArrayList(new Path(m_left.getHdfsPath()));
		PlanMRExecutionMode leftMode = LoadMarmotFile.getExecutionMode(m_marmot, leftPaths);
		switch ( leftMode ) {
			case NON_LOCALLY_EXECUTABLE:
			case NON_MAPREDUCE_EXECUTABLE:
				return leftMode;
			default:
				break;
		}
		
		List<Path> rightPaths = Lists.newArrayList(new Path(m_right.getHdfsPath()));
		PlanMRExecutionMode rightMode = LoadMarmotFile.getExecutionMode(m_marmot, rightPaths);
		switch ( rightMode ) {
			case LOCALLY_EXECUTABLE:
				return leftMode;
			default:
				return rightMode;
		}
	}

	@Override
	public String getInputString() {
		return String.format("%s{%s}x%s{%s}", m_leftDsId, m_leftJoinCols,
											m_rightDsId, m_rightJoinCols);
	}

	@Override
	public RecordSet load() {
		checkInitialized();
		
		TagLoadedJoinKeyColumns tag = new TagLoadedJoinKeyColumns(m_leftDsId, m_leftJoinCols,
																	m_rightDsId, m_rightJoinCols,
																	m_outputColumnsExpr, m_opts);

		JoinPartitions join = new JoinPartitions(m_joinKey, "left", m_left.getRecordSchema(),
													"right", m_right.getRecordSchema(),
													m_outputColumnsExpr, m_opts.joinType());
		
		return RecordSetOperatorChain.from(m_marmot)
									.add(tag)
									.add(join)
									.run();
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
		
		return String.format("%s[%s{%s}x%s{%s}]%s", title, m_leftDsId, m_leftJoinCols,
											m_rightDsId, m_rightJoinCols, output);
	}

	public static LoadHashJoin fromProto(LoadHashJoinProto proto) {
		return new LoadHashJoin(proto.getLeftDataset(),
								proto.getLeftJoinColumns(),
								proto.getRightDataset(),
								proto.getRightJoinColumns(),
								proto.getOutputColumnsExpr(),
								JoinOptions.fromProto(proto.getJoinOptions()));
	}

	@Override
	public LoadHashJoinProto toProto() {
		return LoadHashJoinProto.newBuilder()
						.setLeftDataset(m_leftDsId)
						.setLeftJoinColumns(m_leftJoinCols.toString())
						.setRightDataset(m_rightDsId)
						.setRightJoinColumns(m_rightJoinCols.toString())
						.setOutputColumnsExpr(m_outputColumnsExpr)
						.setJoinOptions(m_opts.toProto())
						.build();
	}
}
