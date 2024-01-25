package marmot.mapreduce.input.spatialjoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import marmot.MarmotInternalException;
import marmot.Record;
import marmot.io.RecordWritable;
import marmot.io.geo.index.GlobalIndexEntry;
import marmot.io.geo.index.SpatialIndexedCluster;
import marmot.mapreduce.MarmotMRContexts;
import marmot.optor.geo.join.IntersectsJoinMatcher;
import marmot.optor.geo.join.SpatialJoinMatchers;
import marmot.optor.support.JoinUtils;
import marmot.optor.support.Match;
import marmot.optor.support.colexpr.ColumnSelectionException;
import marmot.optor.support.colexpr.ColumnSelector;
import marmot.support.DefaultRecord;
import marmot.support.EnvelopeTaggedRecord;
import utils.StopWatch;
import utils.UnitUtils;
import utils.io.IOUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class ClusterJoinReader extends RecordReader<NullWritable, RecordWritable> {
	private static final Logger s_logger = LoggerFactory.getLogger(ClusterJoinReader.class);
	private static final NullWritable NULL = NullWritable.get();
	private static final int PROGRESS_REPORT_INTERVAL = 90;
	
	private IntersectsJoinMatcher m_matcher;
	private Iterator<Match<EnvelopeTaggedRecord>> m_iter;
	private ColumnSelector m_colSelector;
	
	private Record m_joined;
	private RecordWritable m_current;
	private long m_matchCount;
	private StopWatch m_elapsed = StopWatch.create();
	private StopWatch m_interval = StopWatch.create();
	
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
		
		String outputColumns = SpatialIndexJoinFileFormat.getOutputColumns(conf);
		
		try {
			m_colSelector = JoinUtils.createJoinColumnSelector(leftInfo.m_schema, rightInfo.m_schema,
																outputColumns);
			m_joined = DefaultRecord.of(m_colSelector.getRecordSchema());
		}
		catch ( ColumnSelectionException e ) {
			throw new MarmotInternalException("fails to initialize ClusterJoinReader", e);
		}
		
		String matcherStr = SpatialIndexJoinFileFormat.getSpatialJoinMatcher(conf);
		m_matcher = (IntersectsJoinMatcher)SpatialJoinMatchers.parse(matcherStr);
		m_matcher.open(leftInfo.m_geomColIdx, rightInfo.m_geomColIdx, leftInfo.m_srid);
		
		m_iter = FStream.from(clusterMatches)
						.flatMap(m -> m_matcher.match(m.m_left, m.m_right))
						.iterator();
		
		m_matchCount = 0;
		m_elapsed.restart();
		m_interval.restart();
	}

	@Override
	public void close() throws IOException {
		m_matcher.close();
		
		s_logger.info("done: {}", this);
	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		boolean hasNext = m_iter.hasNext();
		if ( hasNext ) {
			Match<EnvelopeTaggedRecord> match = m_iter.next();
			Map<String,Record> binding = Maps.newHashMap();
			binding.put("left", match.m_left.getRecord());
			binding.put("right", match.m_right.getRecord());
			m_colSelector.select(binding, m_joined);
			m_current = RecordWritable.from(m_joined);
			
			++m_matchCount;
		}
		reportProgress();
		
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
	
	@Override
	public String toString() {
		if ( m_elapsed.getElapsedInMillis() > 0 ) {
			long velo = (m_matchCount * 1000) / m_elapsed.getElapsedInMillis();
			return String.format("load_spatial_indexed_join: elapsed=%s matches=%d velo=%d/s",
					m_elapsed.getElapsedMillisString(), m_matchCount, velo);
		}
		else {
			return String.format("load_spatial_indexed_join: elapsed=%s matches=%d",
					m_elapsed.getElapsedMillisString(), m_matchCount);
		}
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
							.map(e -> e.start())
							.minMultiple()
							.get(0).intValue();
		int end = FStream.from(entries)
							.map(e -> (int)(e.start() + e.length()))
							.max().get();
		
		byte[] buffer = new byte[end - begin];
		Path path = new Path(new Path(info.m_clusterDir), packId);
		s_logger.debug("read index block: path={}, [{} - {}], nbytes={}",
						path, UnitUtils.toByteSizeString(begin),
						UnitUtils.toByteSizeString(end), UnitUtils.toByteSizeString(buffer.length));
		try ( FSDataInputStream in = fs.open(path) ) {
			in.seek(begin);
			IOUtils.readFully(in, buffer);
		}
		
		Map<String,SpatialIndexedCluster> clusterMap = Maps.newHashMap();
		List<SpatialIndexedCluster> clusters = new ArrayList<>(entries.size());
		for ( GlobalIndexEntry entry: entries ) {
			SpatialIndexedCluster cluster = clusterMap.get(entry.quadKey());
			if ( cluster == null ) {
				cluster = SpatialIndexedCluster.fromBytes(buffer, (int)(entry.start()-begin),
													(int)entry.length());
				clusterMap.put(entry.quadKey(), cluster);
			}
			clusters.add(cluster);
		}
		
		return clusters;
	}
	
	private void reportProgress() {
		if ( m_interval.getElapsedInSeconds() >= PROGRESS_REPORT_INTERVAL ) {
			s_logger.info("report: {}", this);
			MarmotMRContexts.reportProgress();
			
			m_interval.restart();
		}
	}
}