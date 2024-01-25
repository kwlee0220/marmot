package marmot.optor.geo;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.dataset.DataSetImpl;
import marmot.io.geo.cluster.QuadCluster;
import marmot.io.geo.cluster.QuadClusterFile;
import marmot.io.geo.index.SpatialIndexedFile;
import marmot.mapreduce.PlanMRExecutionMode;
import marmot.mapreduce.input.spatialjoin.ClusterAccessInfo;
import marmot.mapreduce.input.spatialjoin.SpatialIndexJoinFileFormat;
import marmot.optor.MapReduceableRecordSetLoader;
import marmot.optor.RecordSetOperatorException;
import marmot.optor.geo.join.IntersectsJoinMatcher;
import marmot.optor.geo.join.SpatialJoinMatchers;
import marmot.optor.support.AbstractRecordSetLoader;
import marmot.optor.support.JoinUtils;
import marmot.optor.support.Match;
import marmot.optor.support.colexpr.ColumnSelector;
import marmot.plan.SpatialJoinOptions;
import marmot.proto.optor.LoadSpatialIndexJoinProto;
import marmot.rset.AbstractRecordSet;
import marmot.support.EnvelopeTaggedRecord;
import marmot.support.PBSerializable;
import marmot.support.ProgressReportable;
import utils.StopWatch;
import utils.Utilities;
import utils.stream.FStream;

/**
 * {@link LoadSpatialIndexJoin}연산은 지정된 HDFS 파일 (또는 복수개의 HDFS 파일들)을 읽어 저장된
 * 레코드를 읽어 레코드 세트로 생성하는 작업을 수행한다.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LoadSpatialIndexJoin extends AbstractRecordSetLoader
								implements MapReduceableRecordSetLoader,
											PBSerializable<LoadSpatialIndexJoinProto> {
	private final String m_leftDsId;
	private final String m_rightDsId;
	private final SpatialJoinOptions m_options;
	private final SpatialRelation m_joinExpr;
	
	private DataSetImpl m_leftDs;
	private DataSetImpl m_rightDs;
	private ColumnSelector m_selector;
	
	public LoadSpatialIndexJoin(String leftDataSet, String rightDataSet, SpatialJoinOptions opts) {
		Utilities.checkNotNullArgument(leftDataSet, "Left dataset id");
		Utilities.checkNotNullArgument(rightDataSet, "Right dataset id");
		Utilities.checkNotNullArgument(opts, "SpatialJoinOptions is null");

		m_leftDsId = leftDataSet;
		m_rightDsId = rightDataSet;
		m_options = opts;
		m_joinExpr = opts.joinExpr().map(SpatialRelation::parse)
									.getOrElse(SpatialRelation.INTERSECTS);
		switch ( m_joinExpr.getCode() ) {
			case CODE_INTERSECTS:
			case CODE_WITHIN_DISTANCE:
				break;
			default:
				throw new IllegalArgumentException("unsupported join relation: " + m_joinExpr);
		}
		
		setLogger(LoggerFactory.getLogger(LoadSpatialIndexJoin.class));
	}
	
	@Override
	public void initialize(MarmotCore marmot) {
		m_leftDs = marmot.getDataSet(m_leftDsId);
		m_rightDs = marmot.getDataSet(m_rightDsId);
		
		try {
			m_selector = JoinUtils.createJoinColumnSelector(m_leftDs.getRecordSchema(),
															m_rightDs.getRecordSchema(),
															m_options.outputColumns());
			setInitialized(marmot, m_selector.getRecordSchema());
		}
		catch ( Exception e ) {
 			throw new RecordSetOperatorException(String.format("op=%s, cause=%s", this, e));
		}
	}

	@Override
	public String getInputString() {
		return ""+ m_leftDs.getId() + " X " + m_rightDs.getId();
	}

	@Override
	public void configure(Job job) {
		// 입력 파일 포맷 설정
		job.setInputFormatClass(SpatialIndexJoinFileFormat.class);
		
		// 기타 설정
		Configuration conf = job.getConfiguration();
		ClusterAccessInfo leftInfo = ClusterAccessInfo.from(m_leftDs);
		SpatialIndexJoinFileFormat.setLeftClusterAccessInfo(conf, leftInfo);
		ClusterAccessInfo rightInfo = ClusterAccessInfo.from(m_rightDs);
		SpatialIndexJoinFileFormat.setRightClusterAccessInfo(conf, rightInfo);
		SpatialIndexJoinFileFormat.setSpatialJoinMatcher(conf, m_joinExpr.toStringExpr());
		SpatialIndexJoinFileFormat.setOutputColumns(conf, m_selector.getColumnExpression());
		SpatialIndexJoinFileFormat.setIsExistsMatcher(conf, false);
	}

	@Override
	public PlanMRExecutionMode getExecutionMode(MarmotCore marmot) throws IOException {
		SpatialIndexedFile left = (SpatialIndexedFile)m_leftDs.getSpatialIndexFile();
		SpatialIndexedFile right = (SpatialIndexedFile)m_rightDs.getSpatialIndexFile();
		int nmatches = (int)SpatialIndexedFile.matchClusters(left, right).count();
		
		if ( nmatches > marmot.getlocallyExecutableBlockLimitHard()*2 ) {
			return PlanMRExecutionMode.NON_LOCALLY_EXECUTABLE;
		}
		if ( nmatches <= marmot.getLocallyExecutableBlockLimitSoft()*2 ) {
			return PlanMRExecutionMode.LOCALLY_EXECUTABLE;
		}
		else {
			return PlanMRExecutionMode.CONDITIONALLY_EXECUTABLE;
		}
	}
	
	@Override
	public RecordSet load() {
		return new Loaded();
	}
	
	@Override
	public String toString() {
		return String.format("load_spatial_indexed_join[op=%s]: left=%s, right=%s",
							m_joinExpr, m_leftDsId, m_rightDsId);
	}
	
	public void reportProgress(Logger logger, StopWatch elapsed) {
		logger.info("done: {}", this);
	}

	public static LoadSpatialIndexJoin fromProto(LoadSpatialIndexJoinProto proto) {
		SpatialJoinOptions opts = SpatialJoinOptions.fromProto(proto.getOptions());
		return new LoadSpatialIndexJoin(proto.getLeftDataset(), proto.getRightDataset(), opts);
	}

	@Override
	public LoadSpatialIndexJoinProto toProto() {
		return LoadSpatialIndexJoinProto.newBuilder()
										.setLeftDataset(m_leftDsId)
										.setRightDataset(m_rightDsId)
										.setOptions(m_options.toProto())
										.build();
	}

	private class Loaded extends AbstractRecordSet implements ProgressReportable {
		private final SpatialIndexedFile m_leftClusterFile;
		private final SpatialIndexedFile m_rightClusterFile;
		private final int m_leftGeomColIdx;
		private final int m_rightGeomColIdx;
		private final IntersectsJoinMatcher m_matcher;
		
		private Iterator<Match<EnvelopeTaggedRecord>> m_iter;
		private int m_clusterMatchCount = 0;
		private long m_count = 0;
		
		public Loaded() {
			m_leftClusterFile = m_leftDs.getSpatialIndexFile();
			m_rightClusterFile = m_rightDs.getSpatialIndexFile();
			
			m_leftGeomColIdx = m_leftDs.getGeometryColumnIndex();
			m_rightGeomColIdx = m_rightDs.getGeometryColumnIndex();
			
			m_matcher = (IntersectsJoinMatcher)SpatialJoinMatchers.from(m_joinExpr);
		}
		
		@Override protected void closeInGuard() { }
	
		@Override
		public RecordSchema getRecordSchema() {
			return LoadSpatialIndexJoin.this.getRecordSchema();
		}
	
		@Override
		public boolean next(Record record) throws RecordSetException {
			if ( m_iter == null ) {
				FStream<Match<String>> matches = QuadClusterFile.matchClusterKeys(m_leftClusterFile,
																				m_rightClusterFile);
				m_iter = matches.flatMap(this::matchRecord).iterator();
				++m_clusterMatchCount;
			}
			
			if ( !m_iter.hasNext() ) {
				return false;
			}

			Match<EnvelopeTaggedRecord> match = m_iter.next();
			Map<String,Record> binding = Maps.newHashMap();
			binding.put("left", match.m_left.getRecord());
			binding.put("right", match.m_right.getRecord());
			m_selector.select(binding, record);
			++m_count;
			
			return true;
		}
		
		@Override
		public void reportProgress(Logger logger, StopWatch elapsed) {
			logger.info(toString());
		}
		
		@Override
		public String toString() {
			return String.format("%s: cluster_count=%d count=%d", LoadSpatialIndexJoin.this,
								m_clusterMatchCount, m_count);
		}
		
		private FStream<Match<EnvelopeTaggedRecord>> matchRecord(Match<String> match) {
			QuadCluster left = m_leftClusterFile.getCluster(match.m_left);
			QuadCluster right = m_rightClusterFile.getCluster(match.m_right);
			
			m_matcher.open(m_leftGeomColIdx, m_rightGeomColIdx,
							m_leftDs.getGeometryColumnInfo().srid());
			try {
				return m_matcher.match(left, right);
			}
			finally {
				m_matcher.close();
			}
		}
	}
}
