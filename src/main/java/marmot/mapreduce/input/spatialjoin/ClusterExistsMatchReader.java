package marmot.mapreduce.input.spatialjoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import marmot.Record;
import marmot.io.RecordWritable;
import marmot.io.geo.index.GlobalIndexEntry;
import marmot.io.geo.index.SpatialIndexedCluster;
import marmot.mapreduce.MarmotMRContexts;
import marmot.optor.geo.join.IntersectsJoinMatcher;
import marmot.optor.geo.join.SpatialJoinMatchers;
import marmot.optor.support.Match;
import marmot.support.EnvelopeTaggedRecord;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class ClusterExistsMatchReader extends RecordReader<NullWritable, RecordWritable> {
	private static final NullWritable NULL = NullWritable.get();
	
	private IntersectsJoinMatcher m_matcher;
	private Iterator<Record> m_iter;
	
	private RecordWritable m_current;
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
		Configuration conf = context.getConfiguration();
		
		FileSystem fs = FileSystem.get(conf);
		ClusterFileJoinSplit cfjSplit = (ClusterFileJoinSplit)split;
		List<Match<GlobalIndexEntry>> matches = cfjSplit.getMatches();
		
		ClusterAccessInfo leftInfo = SpatialIndexJoinFileFormat.getLeftClusterAccessInfo(conf);
		ClusterAccessInfo rightInfo = SpatialIndexJoinFileFormat.getRightClusterAccessInfo(conf);
		List<Match<SpatialIndexedCluster>> clusterMatches
								= loadClusterMatches(fs, leftInfo, rightInfo, matches);

		String matcherStr = SpatialIndexJoinFileFormat.getSpatialJoinMatcher(conf);
		m_matcher = (IntersectsJoinMatcher)SpatialJoinMatchers.parse(matcherStr);
		m_matcher.open(leftInfo.m_geomColIdx, rightInfo.m_geomColIdx, leftInfo.m_srid);
		
		m_iter = FStream.from(clusterMatches)
						.flatMap(m -> existsMatches(m.m_left, m.m_right))
						.iterator();
	}

	@Override
	public void close() throws IOException {
		m_matcher.close();
		
		MarmotMRContexts.unset();
	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		boolean hasNext = m_iter.hasNext();
		if ( hasNext ) {
			m_current = RecordWritable.from(m_iter.next());
		}
		
		return hasNext;
	}

	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		return NULL;
	}

	@Override
	public RecordWritable getCurrentValue() throws IOException, InterruptedException {
		return m_current;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0f;
	}
	
	private FStream<Record> existsMatches(SpatialIndexedCluster left, SpatialIndexedCluster right) {
		return left.read(false)
					.filter(r -> m_matcher.match(r.getRecord(), right).exists())
					.map(EnvelopeTaggedRecord::getRecord);
	}
	
	private List<Match<SpatialIndexedCluster>>
	loadClusterMatches(FileSystem fs, ClusterAccessInfo info1, ClusterAccessInfo info2,
						List<Match<GlobalIndexEntry>> matches)
		throws IOException {
		List<GlobalIndexEntry> entries1 = FStream.from(matches)
													.map(m -> m.m_left)
													.toList();
		List<SpatialIndexedCluster> clusters1 = loadClusters(fs, info1, entries1);

		List<GlobalIndexEntry> entries2 = FStream.from(matches)
															.map(m -> m.m_right)
															.toList();
		List<SpatialIndexedCluster> clusters2 = loadClusters(fs, info2, entries2);
		
		List<Match<SpatialIndexedCluster>> clusterMatches = new ArrayList<>(clusters1.size());
		for ( int i =0; i < clusters1.size(); ++i ) {
			clusterMatches.add(new Match<>(clusters1.get(i), clusters2.get(i)));
		}
		
		return clusterMatches;
	}
	
	private List<SpatialIndexedCluster> loadClusters(FileSystem fs, ClusterAccessInfo info,
												List<GlobalIndexEntry> entries)
		throws IOException {
		String packId = entries.get(0).packId();
		int begin = FStream.from(entries)
							.map(GlobalIndexEntry::start)
							.minMultiple()
							.get(0).intValue();
		int end = FStream.from(entries)
							.map(e -> (int)(e.start() + e.length()))
							.max().get();
		
		byte[] buffer = new byte[end - begin];
		Path path = new Path(new Path(info.m_clusterDir), packId);
		try ( FSDataInputStream in = fs.open(path) ) {
			in.seek(begin);
			in.read(buffer);
		}
		
		return FStream.from(entries)
					.map(e -> SpatialIndexedCluster.fromBytes(buffer, (int)(e.start()-begin),
														(int)e.length()))
					.toList();
	}
}