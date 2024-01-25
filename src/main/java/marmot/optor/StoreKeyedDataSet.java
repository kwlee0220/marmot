package marmot.optor;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.dataset.Catalog;
import marmot.dataset.DataSetType;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.catalog.DataSetInfo;
import marmot.io.MarmotFileWriteOptions;
import marmot.optor.support.AbstractRecordSetConsumer;
import marmot.optor.support.KeyedRecordSet;
import marmot.proto.optor.StoreKeyedDataSetProto;
import marmot.rset.AbstractRecordSet;
import marmot.support.PBSerializable;
import marmot.support.PeriodicProgressReporter;
import marmot.support.ProgressReportable;
import utils.Utilities;
import utils.func.FOption;
import utils.stream.FStream;
import utils.thread.RecurringScheduleThread;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StoreKeyedDataSet extends AbstractRecordSetConsumer
								implements PBSerializable<StoreKeyedDataSetProto> {
	private final Path m_rootPath;
	private final StoreDataSetOptions m_options;
	
	private String m_dsId;
	private long m_count;
	
	private StoreKeyedDataSet(Path rootPath, StoreDataSetOptions opts) {
		Utilities.checkNotNullArgument(rootPath, "target root path is null");
		Utilities.checkNotNullArgument(opts, "options are null");
		
		m_rootPath = rootPath;
		m_options = opts;
		
		setLogger(LoggerFactory.getLogger(StoreKeyedDataSet.class));
	}

	@Override
	public void consume(RecordSet rset) {
		checkInitialized();
		Preconditions.checkArgument(rset instanceof KeyedRecordSet);
		
		final MarmotCore marmot = getMarmotCore();
		
		KeyedRecordSet group = (KeyedRecordSet)rset;
		String fname = FStream.of(group.getKey().getValues()).map(Object::toString).join("_");
		m_dsId = new Path(m_rootPath, fname).toString();
		
		// 'force' 옵션이 있는 경우는 식별자에 해당하는 미리 삭제한다.
		// 주어진 식별자가 폴더인 경우는 폴더 전체를 삭제한다.
		if ( m_options.force() ) {
			if ( !marmot.deleteDataSet(m_dsId) ) {
				marmot.deleteDir(m_dsId);
			}
		}
		
		Catalog catalog = marmot.getCatalog();
		Path path = catalog.generateFilePath(m_dsId);		
		StoreAsHeapfile store = new StoreAsHeapfile(path, MarmotFileWriteOptions.DEFAULT
																.blockSize(m_options.blockSize()));
		store.initialize(marmot, rset.getRecordSchema());
		
		long reportInterval = marmot.getProgressReportIntervalSeconds()
									.map(TimeUnit.SECONDS::toMillis)
									.getOrElse(-1L);
		if ( reportInterval < 0 ) {
			store(marmot, store, group);
		}
		else {
			RecurringScheduleThread reporter
					= PeriodicProgressReporter.create(ProgressReportable.s_logger, store,  reportInterval);
			try {
				reporter.start();
				store(marmot, store, group);
			}
			catch ( InterruptedException | ExecutionException e ) {
				throw new RuntimeException(e);
			}
			finally {
				reporter.stop(true);
			}
		}
	}
	
	private void store(MarmotCore marmot, StoreAsHeapfile consumer, KeyedRecordSet group) {
		m_count = -1;	
		GeomInfoCollectingRecordSet collect = new GeomInfoCollectingRecordSet(group, m_options.geometryColumnInfo());
		consumer.consume(collect);

		DataSetInfo info = new DataSetInfo(m_dsId, DataSetType.FILE, group.getRecordSchema());
		info.setGeometryColumnInfo(m_options.geometryColumnInfo());
		info.setBounds(collect.m_bounds);
		info.setRecordCount(m_count = collect.m_count);
		info.setUpdatedMillis(System.currentTimeMillis());
		marmot.getCatalog().insertDataSetInfo(info);
		
		if ( getLogger().isInfoEnabled() ) {
			getLogger().info("done: {}", toString());
		}
	}
	
	@Override
	public String toString() {
		String geomStr = m_options.geometryColumnInfo().map(i -> ", geom=" + i).getOrElse("");
		return String.format("store_keyed_dataset[id=%s%s, count=%d]", m_dsId, geomStr, m_count);
	}

	public static StoreKeyedDataSet fromProto(StoreKeyedDataSetProto proto) {
		Path rootPath = new Path(proto.getRootPath());
		StoreDataSetOptions opts =  StoreDataSetOptions.fromProto(proto.getOptions());
		
		return new StoreKeyedDataSet(rootPath, opts);
	}

	@Override
	public StoreKeyedDataSetProto toProto() {
		return StoreKeyedDataSetProto.newBuilder()
									.setRootPath(m_rootPath.toString())
									.setOptions(m_options.toProto())
									.build();
	}

	private static class GeomInfoCollectingRecordSet extends AbstractRecordSet {
		private final RecordSet m_src;
		private final int m_geomColIdx;
		
		private long m_count;
		private Envelope m_bounds;
		
		GeomInfoCollectingRecordSet(RecordSet src, FOption<GeometryColumnInfo> geomInfo) {
			m_src = src;
			
			if ( geomInfo.isPresent() ) {
				GeometryColumnInfo info = geomInfo.get();
				m_geomColIdx = m_src.getRecordSchema()
									.findColumn(info.name())
									.map(Column::ordinal)
									.getOrThrow(() ->
										new IllegalArgumentException("invalid geometry column: name=" + info.name()));
			}
			else {
				m_geomColIdx = -1;
			}
			
			m_count = 0;
			m_bounds = new Envelope();
		}

		@Override
		protected void closeInGuard() {
			m_src.closeQuietly();
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_src.getRecordSchema();
		}

		@Override
		public boolean next(Record output) {
			if ( m_src.next(output) ) {
				collect(output);
				return true;
			}
			else {
				return false;
			}
		}
		
		@Override
		public Record nextCopy() {
			checkNotClosed();
			
			return FOption.ofNullable(m_src.nextCopy())
						.ifPresent(this::collect)
						.getOrNull();
		}
		
		private void collect(Record output) {
			if ( m_geomColIdx >= 0 ) {
				Geometry geom = output.getGeometry(m_geomColIdx);
				if ( geom != null && !geom.isEmpty() ) {
					m_bounds.expandToInclude(geom.getEnvelopeInternal());
				}
			}
			++m_count;
		}
	}
}
