package marmot.optor.geo.join;

import java.util.List;
import java.util.Set;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.dataset.DataSet;
import marmot.dataset.DataSetImpl;
import marmot.io.geo.cluster.CacheableQuadCluster;
import marmot.io.geo.cluster.QuadClusterCache;
import marmot.io.geo.cluster.QuadClusterFile;
import marmot.optor.geo.SpatialRelation;
import marmot.optor.rset.SingleInputRecordSet;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.optor.support.QuadKeyBinder;
import marmot.optor.support.QuadKeyBinder.QuadKeyBinding;
import marmot.plan.SpatialJoinOptions;
import marmot.support.EnvelopeTaggedRecord;
import utils.Utilities;
import utils.func.KeyValue;
import utils.func.Tuple;
import utils.stream.FStream;
import utils.stream.KVFStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class NestedLoopSpatialJoin<T extends NestedLoopSpatialJoin<T>>
															extends AbstractRecordSetFunction {
	public static final Logger s_logger = LoggerFactory.getLogger(NestedLoopSpatialJoin.class);
	private static final long CACHE_SIZE = 5;

	private final String m_geomColName;
	private final String m_paramDsId;
	protected final SpatialJoinOptions m_options;
	
	private final SpatialRelation m_joinExpr;
	private boolean m_semiJoin = false;
	
	// initialized after the call 'initialize()'
	private RecordSchema m_outerSchema;
	private DataSetImpl m_innerDs;
	private Column m_geomCol;
	private Column m_paramGeomCol;

	protected abstract RecordSchema initialize(MarmotCore marmot, RecordSchema outerSchema,
												RecordSchema innerSchema);
	protected abstract RecordSet doInnerLoop(NestedLoopMatch match);
	
	protected NestedLoopSpatialJoin(String geomCol, String paramDsId, SpatialJoinOptions opts) {
		Utilities.checkNotNullArgument(geomCol, "input Geometry column name");
		Utilities.checkNotNullArgument(paramDsId, "inner DataSet id");
		
		m_geomColName = geomCol;
		m_paramDsId = paramDsId;
		m_options = opts;
		m_semiJoin = false;
		m_joinExpr = opts.joinExpr().map(SpatialRelation::parse).getOrElse(SpatialRelation.INTERSECTS);
	}
	
	public String getOuterGeomColumnName() {
		return m_geomColName;
	}
	
	public String getParamDataSetId() {
		return m_paramDsId;
	}
	
	protected DataSet getInnerDataSet() {
		return m_innerDs;
	}
	
	protected Column getInnerGeometryColumn() {
		return m_paramGeomCol;
	}
	
	public SpatialRelation getJoinExpr() {
		return m_joinExpr;
	}
	
	public boolean getSemiJoin() {
		return m_semiJoin;
	}
	
	public SpatialJoinOptions getSpatialJoinOptions() {
		return m_options;
	}
	
	@SuppressWarnings("unchecked")
	public T setSemiJoin(boolean flag) {
		m_semiJoin = flag;
		return (T)this;
	}
	
	public boolean isStreamable() {
		return false;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		m_geomCol = inputSchema.findColumn(m_geomColName)
							.getOrThrow(() -> {
								String details = String.format("invalid input Geometry column: op=%s, col=%s",
																this, m_geomColName);
								return new IllegalArgumentException(details);
							});
		
		m_innerDs = marmot.getDataSetOrNull(m_paramDsId);
		if ( m_innerDs == null ) {
			throw new IllegalArgumentException("Parameter DataSet not found: id="
												+ m_paramDsId + ", op=" + this);
		}
		
		if ( !m_innerDs.hasGeometryColumn() ) {
			String details = String.format("parameter dataset does not have Geometry column: op=%s, ds=%s",
											this, m_innerDs.getId());
			throw new IllegalArgumentException(details);
		}
		
		String geomColName = m_innerDs.getGeometryColumn();
		m_paramGeomCol = m_innerDs.getRecordSchema().findColumn(geomColName)
							.getOrThrow(() -> {
								String details = String.format("invalid parameter Geometry column: op=%s, col=%s",
																this, m_geomColName);
								return new IllegalArgumentException(details);
							});
		
		m_outerSchema = inputSchema;
		RecordSchema outSchema = initialize(marmot, inputSchema, m_innerDs.getRecordSchema());
		setInitialized(marmot, inputSchema, outSchema);
	}
	
	protected final RecordSchema getOuterRecordSchema() {
		return m_outerSchema;
	}
	
	protected final Column getOuterGeomColumn() {
		return m_geomCol;
	}
	
	protected final Geometry getOuterGeometry(Record outerRecord) {
		return outerRecord.getGeometry(m_geomCol.ordinal());
	}
	
	protected final Column getInnerGeomColumn() {
		return m_paramGeomCol;
	}
	
	protected final Geometry getInnerGeometry(Record innerRecord) {
		return innerRecord.getGeometry(getInnerGeomColumn().ordinal());
	}
	
	@Override
	public RecordSet apply(RecordSet rset) {
		return new JoinedRecordSet<>(this, rset);
	}
	
	@Override
	public String toString() {
		return String.format("nested_loop_spatial_join[inner=%s,join=%s]",
							m_paramDsId, m_joinExpr);
	}
	
	protected String toString(long outerCount, long count, long velo, int clusterLoadCount) {
		String str = String.format("%s: nouters=%d, nmatches=%d", this, outerCount, count);
		if ( velo >= 0 ) {
			str = str + String.format(", velo=%d/s", velo);
		}
		str = str + String.format(", load_count=%d", clusterLoadCount);
		
		return str;
	}

	private static int MAX_WINDOW_SIZE = 4 * 1024;
	static class JoinedRecordSet<T extends NestedLoopSpatialJoin<T>>
											extends SingleInputRecordSet<NestedLoopSpatialJoin<T>> {
		private final SpatialJoinMatcher m_sjMatcher;
		private final int m_outerGeomColIdx;

		private final QuadClusterCache m_cache;
		private FStream<Record> m_output;
		private long m_outerCount;
		private long m_count;
		
		public JoinedRecordSet(NestedLoopSpatialJoin<T> join, RecordSet input) {
			super(join, input);
			setLogger(NestedLoopSpatialJoin.s_logger);
					
			m_outerGeomColIdx = join.m_geomCol.ordinal();
			
			m_sjMatcher = SpatialJoinMatchers.from(join.m_joinExpr);
			m_sjMatcher.open(m_outerGeomColIdx, join.m_paramGeomCol.ordinal(),
							join.m_innerDs.getGeometryColumnInfo().srid());

			QuadClusterFile<? extends CacheableQuadCluster> idxFile
										= join.m_innerDs.loadOrBuildQuadClusterFile(join.m_marmot);
			m_cache = new QuadClusterCache(idxFile, CACHE_SIZE);
			
			Set<String> qkSrc = m_cache.getClusterKeyAll();
			QuadKeyBinder qkBinder = new QuadKeyBinder(qkSrc, false);

			m_output = m_input.fstream()
							.map(outer -> sortOut(qkBinder, outer))
							.filter(kv -> kv.key() != 0)
							.toKeyValueStream(kv -> kv)
							.findBiggestGroupWithinWindow(2 * MAX_WINDOW_SIZE)
							.flatMap(kv -> kv.key() == 1 ? joinWithClustering(kv.value()) : joinWithoutClustering(kv.value()));
		}
		
		@Override
		protected void closeInGuard() {
			m_sjMatcher.close();
			m_cache.cleanUp();
			
			super.closeInGuard();
		}
		
		@Override
		public Record nextCopy() {
			checkNotClosed();
			
			return m_output.next()
							.ifPresent(r -> ++m_count)
							.getOrNull();
		}
		
		@Override
		public String toString() {
			int clusterLoadCount = m_cache.getLoadCount();
			
			if ( m_elapsed > 0 ) {
				long veloc = (m_outerCount*1000) / m_elapsed;
				return m_optor.toString(m_outerCount, m_count, veloc, clusterLoadCount);
			}
			else {
				return m_optor.toString(m_outerCount, m_count, -1, clusterLoadCount);
			}
		}
		
		private FStream<Record> joinWithClustering(List<Tuple<Record,List<String>>> list) {
			return KVFStream.fromTupleList(list)
							.map(kv -> KeyValue.of(kv.value().get(0), kv.key()))
							.toKeyValueStream(kv -> kv)
							.findBiggestGroupWithinWindow(MAX_WINDOW_SIZE, 1)
							.map(kv -> new ClusteringNLJoinJob(kv.key(), kv.value()))
							.flatMap(job -> job.run());
		}
		
		private class ClusteringNLJoinJob {
			private String m_quadKey;
			private List<Record> m_outers;
			private int m_inputCount = 0;
			
			ClusteringNLJoinJob(String quadKey, List<Record> outers) {
				m_quadKey = quadKey;
				m_outers = outers;
			}
			
			public FStream<Record> run() {
				if ( getLogger().isDebugEnabled() ) {
					getLogger().debug(toString());
				}
				return FStream.from(m_outers)
								.peek(r -> ++m_inputCount)
								.map(outer -> matchOuter(outer))
								.flatMap(m -> m_optor.doInnerLoop(m).fstream());
			}
			
			@Override
			public String toString() {
				return String.format("%s: cluster=%s, progress=%d/%d (total=%d -> %d, load=%d)", getClass().getSimpleName(),
									m_quadKey, m_inputCount, m_outers.size(),
									m_outerCount, m_count, m_cache.getLoadCount());
			}
		}
		
		private FStream<Record> joinWithoutClustering(List<Tuple<Record,List<String>>> tuples) {
			List<Tuple<Record,List<String>>> records = tuples;
			List<NonClusteringNLJoinJob> jobs = Lists.newArrayList();
			
			while ( records.size() > 0 ) {
				KeyValue<String,List<Record>> selected
									= FStream.from(records)
											.flatMap(t -> FStream.from(t._2)
																.map(k -> KeyValue.of(k, t._1)))
											.groupByKey(kv -> kv.key(), kv -> kv.value())
											.stream()
											.max(kv -> kv.value().size())
											.get();
				
				jobs.add(new NonClusteringNLJoinJob(selected.key(), selected.value()));
				records = FStream.from(records)
								.filter(t -> !t._2.contains(selected.key()))
								.toList();
			}

			return FStream.from(jobs).flatMap(job -> job.run());
		}
		
		private class NonClusteringNLJoinJob {
			private String m_quadKey;
			private List<Record> m_outers;
			private int m_inputCount = 0;
			
			NonClusteringNLJoinJob(String quadKey, List<Record> outers) {
				m_quadKey = quadKey;
				m_outers = outers;
			}
			
			public FStream<Record> run() {
				if ( getLogger().isDebugEnabled() ) {
					getLogger().debug(toString());
				}
				return FStream.from(m_outers)
								.peek(r -> ++m_inputCount)
								.map(outer -> matchOuter(outer))
								.flatMap(m -> m_optor.doInnerLoop(m).fstream());
			}
			
			@Override
			public String toString() {
				return String.format("%s: head=%s, progress=%d/%d (total=%d -> %d, load=%d)", getClass().getSimpleName(),
									m_quadKey, m_inputCount, m_outers.size(),
									m_outerCount, m_count, m_cache.getLoadCount());
			}
		}
		
		private KeyValue<Integer,Tuple<Record,List<String>>>
		sortOut(QuadKeyBinder qkBinder, Record outer) {
			Geometry geom = outer.getGeometry(m_outerGeomColIdx);
			
			// 공간 정보가 없는 경우는 무시한다.
			if ( geom == null || geom.isEmpty() ) {
				return KeyValue.of(0, Tuple.of(outer, null));
			}
			
			// 조인조건에 따른 검색 키를 생성하고, 이를 바탕으로 관련된
			// inner cluster를 검색한다.
			Envelope envl84 = m_sjMatcher.toMatchKey(geom);
			List<QuadKeyBinding> bindings = qkBinder.bindQuadKeys(envl84);
			List<String> qkeys = FStream.from(bindings).map(QuadKeyBinding::quadkey).toList();
			return KeyValue.of(Math.min(bindings.size(),2), Tuple.of(outer,qkeys));
		}
		
		private NestedLoopMatch matchOuter(Record outer) {
			Geometry outerGeom = outer.getGeometry(m_outerGeomColIdx);
			if ( outerGeom == null || outerGeom.isEmpty() ) {
				return new NestedLoopMatch(outer, m_optor.m_innerDs.getRecordSchema(),
											FStream.empty());
			}
			
			Envelope key = m_sjMatcher.toMatchKey(outerGeom);
			FStream<Record> inners = m_cache.queryClusterKeys(key)
											.map(m_cache::getCluster)
											.flatMap(cluster -> m_sjMatcher.match(outer, cluster))
											.map(EnvelopeTaggedRecord::getRecord)
											.mapIf(m_optor.m_semiJoin, fstrm -> fstrm.take(1));
			++m_outerCount;
			
			return new NestedLoopMatch(outer, m_optor.m_innerDs.getRecordSchema(), inners);
		}
	}
}
