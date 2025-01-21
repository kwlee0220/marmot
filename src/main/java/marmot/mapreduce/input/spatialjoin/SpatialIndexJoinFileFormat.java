package marmot.mapreduce.input.spatialjoin;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.func.FOption;
import utils.func.Tuple;
import utils.func.Tuple4;
import utils.stream.FStream;
import utils.stream.KeyedGroups;

import marmot.io.RecordWritable;
import marmot.io.geo.index.GlobalIndex;
import marmot.io.geo.index.GlobalIndexEntry;
import marmot.io.serializer.MarmotSerializer;
import marmot.optor.geo.SpatialQueryOperation;
import marmot.optor.support.Match;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialIndexJoinFileFormat extends FileInputFormat<NullWritable, RecordWritable> {
	private static final Logger s_logger = LoggerFactory.getLogger(SpatialIndexJoinFileFormat.class);
	private static final MarmotSerializer<ClusterAccessInfo> s_serializer = ClusterAccessInfo.getSerializer();

	private static final String PROP_LEFT_CLUSTER_ACCESS_INFO = "marmot.geo.optor.join.left_cluster_info";
	private static final String PROP_RIGHT_CLUSTER_ACCESS_INFO = "marmot.geo.optor.join.right_cluster_info";
	private static final String PROP_JOIN_MATCHER = "marmot.geo.optor.join.matcher";
	private static final String PROP_IS_EXISTS_MATCHER = "marmot.geo.optor.join.is_exists_matcher";
	private static final String PROP_INDEX_QUERY_OP = "marmot.geo.optor.join.index_query_op";
	private static final String PROP_OUTPUT_COLUMNS = "marmot.optor.join.output_columns";

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		Configuration conf = job.getConfiguration();
		
		ClusterAccessInfo leftInfo = getLeftClusterAccessInfo(conf);
		ClusterAccessInfo rightInfo = getRightClusterAccessInfo(conf);
		
		GlobalIndex leftIdxFile = leftInfo.getIndexFile(conf); 
		GlobalIndex rightIdxFile = rightInfo.getIndexFile(conf);
		
		List<InputSplit> splits = toJoinSplits(leftIdxFile, rightIdxFile);
		s_logger.info("nsplits={}", splits.size());
		
		return splits;
	}

	@Override
	public RecordReader<NullWritable, RecordWritable> createRecordReader(InputSplit split,
																	TaskAttemptContext context)
		throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		
		return getIsExistsMatcher(conf)
				? new ClusterExistsMatchReader()
				: new ClusterJoinReader();
	}
	
	public static String getSpatialJoinMatcher(Configuration conf) {
		return conf.get(PROP_JOIN_MATCHER);
	}
	
	public static void setSpatialJoinMatcher(Configuration conf, String matcherStr) {
		conf.set(PROP_JOIN_MATCHER, matcherStr);
	}
	
	public static SpatialQueryOperation getIndexQueryOperation(Configuration conf) {
		String opStr = conf.get(PROP_INDEX_QUERY_OP);
		return SpatialQueryOperation.valueOf(opStr);
	}
	
	public static void setIndexQueryOperation(Configuration conf, SpatialQueryOperation op) {
		conf.set(PROP_INDEX_QUERY_OP, op.toString());
	}
	
	public static String getOutputColumns(Configuration conf) {
		return conf.get(PROP_OUTPUT_COLUMNS);
	}
	
	public static void setOutputColumns(Configuration conf, String columnSelection) {
		conf.set(PROP_OUTPUT_COLUMNS, columnSelection);
	}
	
	public static boolean getIsExistsMatcher(Configuration conf) {
		return FOption.map(conf.get(PROP_IS_EXISTS_MATCHER),
							str -> Boolean.parseBoolean(str));
	}
	
	public static void setIsExistsMatcher(Configuration conf, boolean flag) {
		conf.set(PROP_IS_EXISTS_MATCHER, ""+flag);
	}
	
	public static ClusterAccessInfo getLeftClusterAccessInfo(Configuration conf)
		throws IOException {
		String base64Str = conf.get(PROP_LEFT_CLUSTER_ACCESS_INFO);
		if ( base64Str != null ) {
			return s_serializer.fromBase64String(base64Str);
		}
		else {
			return null;
		}
	}
	
	public static void setLeftClusterAccessInfo(Configuration conf, ClusterAccessInfo info) {
		conf.set(PROP_LEFT_CLUSTER_ACCESS_INFO, s_serializer.toBase64String(info));
	}
	
	public static ClusterAccessInfo getRightClusterAccessInfo(Configuration conf)
		throws IOException {
		String base64Str = conf.get(PROP_RIGHT_CLUSTER_ACCESS_INFO);
		if ( base64Str != null ) {
			return s_serializer.fromBase64String(base64Str);
		}
		else {
			return null;
		}
	}
	
	public static void setRightClusterAccessInfo(Configuration conf, ClusterAccessInfo info) {
		conf.set(PROP_RIGHT_CLUSTER_ACCESS_INFO, s_serializer.toBase64String(info));
	}
	
	private List<InputSplit>
	toJoinSplits(GlobalIndex leftIdxFile, GlobalIndex rightIdxFile) {
		List<Match<GlobalIndexEntry>> matches
					= GlobalIndex.matchClusters(leftIdxFile, rightIdxFile).toList();
		KeyedGroups<Tuple4<String,Integer,String,Integer>,Match<GlobalIndexEntry>> groups
					= FStream.from(matches)
							.groupByKey(this::toKey);
		return groups.stream()
					.toValueStream()
					.map(ClusterFileJoinSplit::new)
					.cast(InputSplit.class)
					.toList();
	}
	
	private Tuple4<String,Integer,String,Integer> toKey(Match<GlobalIndexEntry> match) {
		return Tuple.of(match.m_left.packId(), match.m_left.blockNo(), match.m_right.packId(), match.m_right.blockNo());
	}
}
