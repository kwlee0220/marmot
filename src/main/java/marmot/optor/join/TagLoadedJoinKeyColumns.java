package marmot.optor.join;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.dataset.DataSet;
import marmot.io.HdfsPath;
import marmot.io.MultiColumnKey;
import marmot.io.RecordWritable;
import marmot.mapreduce.PlanMRExecutionMode;
import marmot.mapreduce.input.hashjoin.HashJoinInputFormat.Parameters;
import marmot.mapreduce.input.hashjoin.HashJoinLeftInputFormat;
import marmot.mapreduce.input.hashjoin.HashJoinLeftMRMapper;
import marmot.mapreduce.input.hashjoin.HashJoinRightInputFormat;
import marmot.mapreduce.input.hashjoin.HashJoinRightMRMapper;
import marmot.optor.JoinOptions;
import marmot.optor.JoinType;
import marmot.optor.LoadMarmotFile;
import marmot.optor.MapReduceableRecordSetLoader;
import marmot.optor.RecordSetOperatorException;
import marmot.optor.SortOrder;
import marmot.optor.geo.LoadSpatialIndexJoin;
import marmot.optor.support.AbstractRecordSetLoader;
import marmot.proto.optor.LoadHashJoinProto;
import marmot.rset.AbstractRecordSet;
import marmot.support.DefaultRecord;
import marmot.support.PBSerializable;
import marmot.type.DataType;
import utils.CSV;
import utils.Utilities;
import utils.func.UncheckedFunction;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TagLoadedJoinKeyColumns extends AbstractRecordSetLoader
									implements MapReduceableRecordSetLoader,
												PBSerializable<LoadHashJoinProto> {
	public static final String KEY_COL = "cacheable";
	public static final MultiColumnKey ORDER_KEY = MultiColumnKey.of(KEY_COL, SortOrder.DESC);
	
	private final String m_leftDsId;
	private final String m_rightDsId;
	private final String m_leftJoinCols;
	private final String m_rightJoinCols;
	private final String m_outputColumnsExpr;
	private final JoinOptions m_opts;
	
	private DataSet m_leftDs;
	private DataSet m_rightDs;
	
	TagLoadedJoinKeyColumns(String leftDataSet, String leftJoinCols,
							String rightDataSet, String rightJoinCols,
							String outputColumns, JoinOptions opts) {
		Utilities.checkNotNullArgument(leftDataSet,  "left dataset id is null");
		Utilities.checkNotNullArgument(rightDataSet,  "right dataset id is null");
		Utilities.checkArgument(leftJoinCols != null, "left join columns are null");
		Utilities.checkArgument(rightJoinCols != null, "right join columns are null");
		
		String[] leftJoinColNames = CSV.parseCsvAsArray(leftJoinCols);
		String[] rightJoinColNames = CSV.parseCsvAsArray(rightJoinCols);
		Utilities.checkArgument(leftJoinColNames.length > 0, "left join columns are empty");
		Utilities.checkArgument(rightJoinColNames.length > 0, "right join columns are empty");
		Utilities.checkArgument(leftJoinColNames.length == rightJoinColNames.length,
									"both join columns does not match");
		Preconditions.checkArgument(outputColumns != null,  "output columns is null");
		Preconditions.checkArgument(opts != null,  "JoinOptions is null");
		
		m_leftDsId = leftDataSet;
		m_rightDsId = rightDataSet;
		m_leftJoinCols = leftJoinCols;
		m_rightJoinCols = rightJoinCols;
		m_outputColumnsExpr = outputColumns;
		m_opts = opts;
		
		setLogger(LoggerFactory.getLogger(LoadSpatialIndexJoin.class));
	}
	
	public String getLeftJoinColumns() {
		return m_leftJoinCols;
	}
	
	public String getRightJoinColumns() {
		return m_rightJoinCols;
	}

	@Override
	public void initialize(MarmotCore marmot) {
		m_leftDs = marmot.getDataSet(m_leftDsId);
		m_rightDs = marmot.getDataSet(m_rightDsId);
		RecordSchema leftSchema = m_leftDs.getRecordSchema();
		
		RecordSchema.Builder builder = MapReduceHashJoin.SCHEMA_PREFIX.toBuilder();
		String[] leftJoinColNames = CSV.parseCsvAsArray(m_leftJoinCols);
		for ( int i = 0; i < leftJoinColNames.length; ++i ) {
			String jColName = String.format("jc%02d", i);
			DataType jColType = leftSchema.getColumn(leftJoinColNames[i]).type();
			builder.addColumn(jColName, jColType);
		}
		
		setInitialized(marmot, builder.build());
	}

	@Override
	public void configure(Job job) {
		checkInitialized();

		try {
			FileSystem fs = m_marmot.getHadoopFileSystem();
			
			// left input dataset 처리
			long leftSize = HdfsPath.of(fs, new Path(m_leftDs.getHdfsPath()))
									.walkRegularFileTree()
									.peek(path -> {
										getLogger().debug("left input file: {}", path);
										MultipleInputs.addInputPath(job, path.getPath(),
																	HashJoinLeftInputFormat.class,
																	HashJoinLeftMRMapper.class);
									})
									.map(UncheckedFunction.sneakyThrow(HdfsPath::getFileStatus))
									.mapToLong(FileStatus::getLen)
									.sum();
			
			// right input dataset 처리
			long rightSize = HdfsPath.of(fs, new Path(m_rightDs.getHdfsPath()))
									.walkRegularFileTree()
									.peek(path -> {
										getLogger().debug("right input file: {}", path);
										MultipleInputs.addInputPath(job, path.getPath(),
																	HashJoinRightInputFormat.class,
																	HashJoinRightMRMapper.class);
									})
									.map(UncheckedFunction.sneakyThrow(HdfsPath::getFileStatus))
									.mapToLong(FileStatus::getLen)
									.sum();
			
			boolean cacheLeft = leftSize < rightSize;
			
			Configuration conf = job.getConfiguration();
			Parameters leftParams = new Parameters(m_leftDs.getRecordSchema(),
													m_leftJoinCols, cacheLeft);
			HashJoinLeftInputFormat.setParameters(conf, leftParams);

			Parameters rightParams = new Parameters(m_rightDs.getRecordSchema(),
													m_rightJoinCols, !cacheLeft);
			HashJoinRightInputFormat.setParameters(conf, rightParams);
		}
		catch ( IOException e ) {
			throw new RecordSetOperatorException("fails to initialize operator=" + getClass(), e);
		}
	}

	@Override
	public PlanMRExecutionMode getExecutionMode(MarmotCore marmot) throws IOException {
		checkInitialized();
		
		List<Path> leftPaths = Lists.newArrayList(new Path(m_leftDs.getHdfsPath()));
		PlanMRExecutionMode leftMode = LoadMarmotFile.getExecutionMode(m_marmot, leftPaths);
		switch ( leftMode ) {
			case NON_LOCALLY_EXECUTABLE:
			case NON_MAPREDUCE_EXECUTABLE:
				return leftMode;
			default:
				break;
		}
		
		List<Path> rightPaths = Lists.newArrayList(new Path(m_rightDs.getHdfsPath()));
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
		return new TaggedRecordSet(this);
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

	public static TagLoadedJoinKeyColumns fromProto(LoadHashJoinProto proto) {
		return new TagLoadedJoinKeyColumns(
									proto.getLeftDataset(), proto.getLeftJoinColumns(),
									proto.getRightDataset(), proto.getRightJoinColumns(),
									proto.getOutputColumnsExpr(),
									JoinOptions.fromProto(proto.getJoinOptions()));
	}

	@Override
	public LoadHashJoinProto toProto() {
		return LoadHashJoinProto.newBuilder()
								.setLeftDataset(m_leftDsId)
								.setLeftJoinColumns(m_leftJoinCols)
								.setRightDataset(m_rightDsId)
								.setRightJoinColumns(m_rightJoinCols)
								.setOutputColumnsExpr(m_outputColumnsExpr)
								.setJoinOptions(m_opts.toProto())
								.build();
	}
	
	private static class TaggedRecordSet extends AbstractRecordSet {
		private final TagLoadedJoinKeyColumns m_tag;
		private int m_dsIdx;
		private RecordSet m_source;
		private Record m_inputRecord;
		private int[] m_joinColIdxes;
		
		private TaggedRecordSet(TagLoadedJoinKeyColumns tag) {
			m_tag = tag;

			m_dsIdx = 0;
			setup(m_tag.m_leftDs, m_tag.getLeftJoinColumns());
		}
		
		@Override
		protected void closeInGuard() {
			if ( m_source != null ) {
				m_source.closeQuietly();
				m_source = null;
			}
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_tag.getRecordSchema();
		}
		
		@Override
		public boolean next(Record output) {
			while ( m_dsIdx <= 1 ) {
				if ( m_source.next(m_inputRecord) ) {
					output.set(MapReduceHashJoin.TAG_IDX_DATASET_IDX, (byte)m_dsIdx);
					output.set(MapReduceHashJoin.TAG_IDX_CACHEABLE, (byte)m_dsIdx);
					output.set(MapReduceHashJoin.TAG_IDX_DECODED,
								RecordWritable.from(m_inputRecord).toBytes());
					
					int jcIdx = 3;
					for ( int colIdx: m_joinColIdxes ) {
						output.set(jcIdx++, m_inputRecord.get(colIdx));
					}
					
					return true;
				}
				m_source.closeQuietly();
				m_source = null;
				
				if ( m_dsIdx >= 1 ) {
					return false;
				}
				
				setup(m_tag.m_rightDs, m_tag.m_rightJoinCols);
				m_dsIdx = 1;
			}
			
			return false;
		}
		
		private void setup(DataSet ds, String jcols) {
			m_source = ds.read();
			m_inputRecord = DefaultRecord.of(m_source.getRecordSchema());

			RecordSchema inputSchema = m_source.getRecordSchema();
			m_joinColIdxes = CSV.parseCsv(jcols)
								.mapToInt(colName -> inputSchema.getColumn(colName).ordinal())
								.toArray();
		}
	}
}
