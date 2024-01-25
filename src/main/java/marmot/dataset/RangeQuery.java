package marmot.dataset;

import static utils.Utilities.checkNotNullArgument;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.StopWatch;
import utils.Throwables;
import utils.stream.FStream;
import utils.stream.SuppliableFStream;

import marmot.MarmotCore;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.geo.GeoClientUtils;
import marmot.geo.query.RangeQueryEstimate;
import marmot.geo.query.RangeQueryEstimate.ClusterEstimate;
import marmot.io.geo.cluster.QuadCluster;
import marmot.io.geo.index.SpatialIndexedFile;
import marmot.optor.AggregateFunction;
import marmot.optor.StoreDataSetOptions;
import marmot.support.EnvelopeTaggedRecord;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RangeQuery {
	private static final Logger s_logger = LoggerFactory.getLogger(RangeQuery.class);
	private static final int LOCAL_INDEX_SCAN_THREADS = 3;
	private static final int LOCAL_INDEX_SCAN_THRESHOLD = 25;
	
	private final MarmotCore m_marmot;
	private final DataSetImpl m_ds;
	private final int m_geomColIdx;
	private final Envelope m_range;
	private final int m_sampleCount;
	
	private final Envelope m_rangeWgs84;
	private final PreparedGeometry m_pkey;
	
	RangeQuery(MarmotCore marmot, DataSetImpl ds, Envelope range, int sampleCount) {
		checkNotNullArgument(marmot, "MarmotRuntime");
		checkNotNullArgument(ds, "DataSet");
		checkNotNullArgument(range, "query range");
		
		m_marmot = marmot;
		m_ds = ds;
		m_range = range;
		m_sampleCount = sampleCount;
		
		m_rangeWgs84 = m_ds.toWgs84(m_range);
		m_pkey = PreparedGeometryFactory.prepare(GeoClientUtils.toPolygon(range));
		m_geomColIdx = m_ds.getGeometryColumnIndex();
	}
	
	/**
	 * 영역 질의에 수행한 결과 레코드 세트를 반환한다.
	 * 
	 * @return	레코드 세트 객체
	 */
	public RecordSet run() {
		StopWatch watch = StopWatch.start();
		try {
			// 대상 DataSet에 인덱스가 걸려있지 않는 경우에도 full scan 방식을 사용한다.
			// 또는 질의 영역이 DataSet 전체 영역보다 더 넓은 경우는 인덱스를 사용하는 방법보다
			// 그냥 전체를 읽는 방식을 효과적이기 때문에 이 방법을  사용한다.
			// 이때 thumbnail이 존재하는 경우에는 이것을 사용하고, 그렇지 않는 경우에만
			// full scan을 사용한다.
			//
			if ( m_range.contains(m_ds.getBounds()) || !m_ds.hasSpatialIndex() ) {
				if ( m_ds.hasThumbnail()
					&& m_sampleCount < (m_ds.getThumbnailRatio().get() * m_ds.getRecordCount()) ) {
					s_logger.info("use thumbnail scan: id={}", m_ds.getId());
					return m_ds.readThumbnail(m_range, m_sampleCount);
				}
				else {
					s_logger.info("use full scan: id={}, nsamples={}", m_ds.getId(), m_sampleCount);
					return fullScan(-1d);
				}
			}
			else {
				// 질의 영역과 겹치는 quad-key들과, 해당 결과 레코드의 수를 추정하여
				// 그에 따른 질의처리를 시도한다.
				return indexBasedScan();
			}
		}
		catch ( Throwable e ) {
			throw Throwables.toRuntimeException(Throwables.unwrapThrowable(e));
		}
		finally {
			watch.stop();
			s_logger.debug("RangeQuery: dataset={}, elapsed={}",
							m_ds.getId(), watch.getElapsedMillisString());
		}
	}

	private RecordSet indexBasedScan() throws IOException {
		// 질의 영역과 겹치는 클러스터들과, 추정되는 결과 레코드의 수를 계산한다.
		RangeQueryEstimate est = m_ds.estimateRangeQuery(m_range);
		
		// 추정된 결과 레코드 수를 통해 샘플링 비율을 계산한다.
		double ratio = (m_sampleCount > 0) ? (double)m_sampleCount / est.getMatchCount() : 1d;
		final double sampleRatio = Math.min(ratio, 1);
		
		String msg = String.format("ds_id=%s, ratio=%.3f", m_ds.getId(), sampleRatio);
		
		if ( m_ds.hasThumbnail() && (m_ds.getThumbnailRatio().get() > sampleRatio) ) {
			msg = String.format("%s, thumbnail_ratio=%.3f", msg, m_ds.getThumbnailRatio());
			s_logger.info("use thumbnail: {}, ", msg);
			
			return m_ds.readThumbnail(m_range, m_sampleCount);
		}
//		else if ( est.getClusterEstimates().size() <= LOCAL_INDEX_SCAN_THRESHOLD ) {
//			s_logger.info("use local-index-scan: {}, clusters={}", msg, est.getClusterEstimates().size());
//			
//			return localIndexScan(est, sampleRatio);
//		}
		else {
			s_logger.info("use index-scan: {}, clusters={}", msg, est.getClusterEstimates().size());
			
			// 샘플 수가 정의되지 않거나, 대상 데이터세트의 레코드 갯수가 샘플 수보다 작은 경우
			// 데이터세트 전체를 반환한다. 성능을 위해 query() 연산 활용함.
			String planName = String.format("index_scan(ratio=%.3f)", sampleRatio);
			PlanBuilder builder = Plan.builder(planName)
											.query(m_ds.getId(), m_range);
			if ( sampleRatio < 1 ) {
				builder = builder.sample(sampleRatio);
			}
//			if ( m_sampleCount > 0 ) {
//				builder = builder.take(m_sampleCount);
//			}
			Plan plan = builder.build();
			
			return m_marmot.executeToRecordSet(plan);
		}
	}
	
	private RecordSet fullScan(double ratio) {
		Plan plan;
		
		if ( m_sampleCount <= 0 ) {
			// 샘플 수가 지정되지 않은 경우는 전체 질의 결과를 반환한다.
			plan = Plan.builder("full_scan: " + m_ds.getId())
							.query(m_ds.getId(), m_range)
							.build();
			return m_marmot.executeToRecordSet(plan);
		}
		
		if ( ratio < 0 ) {	// 샘플 비율이 지정되지 않은 경우
			// 만일 전체 데이터세트의 레코드 수가 샘플 수에 비해서 크게 많지 않다면
			// 먼저 질의 결과 데이터세트를 구해서 여기서 샘플링 비율을 구하고
			// 바로 샘플링을 실시한다.
			if ( m_ds.getRecordCount() < m_sampleCount * 3 ) {
				return fullScanSmallDataSet(m_range);
			}
			else {
				// 전체 질의 결과 레코드 수를 먼저 알아내어 이를 통해 샘플 비율을 계산한다.
				//
				plan = Plan.builder("findMatchCount_" + m_ds.getId())
								.query(m_ds.getId(), m_range)
								.aggregate(AggregateFunction.COUNT())
								.build();
				long count = m_marmot.executeToLong(plan).get();
				ratio = Math.min(1, (double)m_sampleCount / count);
			}
		}
		
		plan = Plan.builder("SampleRangeQuery_ds=" + m_ds.getId())
						.query(m_ds.getId(), m_range)
						.sample(ratio)
						.build();
		return m_marmot.executeToRecordSet(plan);
	}
	
	private RecordSet fullScanSmallDataSet(Envelope key) {
		String tempDsId = "/tmp/" + UUID.randomUUID().toString();
		
		try {
			Plan plan;
			
			plan = Plan.builder("full_scan: " + m_ds.getId())
						.query(m_ds.getId(), key)
						.store(tempDsId, StoreDataSetOptions.FORCE)
						.build();
			m_marmot.execute(plan);
			
			DataSet tempDs = m_marmot.getDataSet(tempDsId);
			double ratio = Math.min(1, (double)m_sampleCount / tempDs.getRecordCount());
			RecordSet result;
			if ( ratio < 1 ) {
				plan = Plan.builder("sample over temp dataset: " + tempDsId)
								.load(tempDsId)
								.sample(ratio)
								.build();
				result = m_marmot.executeToRecordSet(plan);
			}
			else {
				result = m_ds.read();
			}
			
			// 결과 RecordSet을 close할 때 임시 DataSet도 함께 삭제되도록 한다.
			return result.onClose(() -> m_marmot.deleteDataSet(tempDsId));
		}
		catch ( Exception e ) {
			m_marmot.deleteDataSet(tempDsId);
			throw e;
		}
	}
	
	private RecordSet localIndexScan(RangeQueryEstimate est, double sampleRatio) {
		SpatialIndexedFile idxFile = m_ds.getSpatialIndexFile();
		RecordSchema schema = m_ds.getRecordSchema();
		
		ExecutorService exector = Executors.newFixedThreadPool(LOCAL_INDEX_SCAN_THREADS);
		CompletionService<List<Record>> svc = new ExecutorCompletionService<>(exector);
		FStream.from(est.getClusterEstimates())
				.zipWithIndex()
				.map(t -> new ClusterReader(t._2, idxFile, t._1, sampleRatio))
				.forEach(svc::submit);
		
		SuppliableFStream<List<Record>> recListStream = FStream.pipe(LOCAL_INDEX_SCAN_THREADS);
		CompletableFuture.runAsync(() -> {
			int remains = est.getClusterEstimates().size();
			while ( remains > 0 ) {
				try {
					List<Record> block = svc.take().get();
					recListStream.supply(block);
				}
				catch ( Throwable e ) {
					e.printStackTrace();
				}
				--remains;
			}
			recListStream.endOfSupply();
		});
		return RecordSet.from(schema, recListStream.flatMap(list -> FStream.from(list)))
							.onClose(exector::shutdown);
	}
	
	private class ClusterReader implements Callable<List<Record>> {
		private final int m_idx;
		private SpatialIndexedFile m_clusterFile;
		private final ClusterEstimate m_cluster;
		private final double m_sampleRatio;
		
		ClusterReader(int idx, SpatialIndexedFile clusterFile, ClusterEstimate cluster, double sampleRatio) {
			m_idx = idx;
			m_clusterFile = clusterFile;
			m_cluster = cluster;
			m_sampleRatio = sampleRatio;
		}
		
		@Override
		public List<Record> call() {
			s_logger.debug("started: {}, thread={}", m_idx, Thread.currentThread().getId());
			StopWatch watch = StopWatch.start();
			
			QuadCluster cluster = m_clusterFile.getCluster(m_cluster.getQuadKey());
			List<Record> rlist = cluster.query(m_rangeWgs84, true)
										.map(EnvelopeTaggedRecord::getRecord)
										.filter(this::matches)
										.sample(m_sampleRatio)
										.toList();
			cluster = null;
			m_clusterFile = null;
			watch.stop();
			
			s_logger.debug("\tmatches: {}: cluster={}, count={}, elapsed={}, thread={}",
							m_idx, m_cluster.getQuadKey(), rlist.size(), watch.getElapsedMillisString(),
							Thread.currentThread().getId());
			return rlist;
		}
		
		private boolean matches(Record rec) {
			return m_pkey.intersects(rec.getGeometry(m_geomColIdx));
		}
	}
}
