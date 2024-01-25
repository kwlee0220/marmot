package marmot.optor.geo.index;

import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.LoggerFactory;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.dataset.DataSet;
import marmot.dataset.IndexNotFoundException;
import marmot.geo.CoordinateTransform;
import marmot.geo.catalog.SpatialIndexInfo;
import marmot.io.HdfsPath;
import marmot.io.geo.index.GlobalIndex;
import marmot.io.geo.index.GlobalIndexEntry;
import marmot.optor.support.AbstractRecordSetLoader;
import marmot.proto.optor.LoadSpatialGlobalIndexProto;
import marmot.rset.AbstractRecordSet;
import marmot.support.PBSerializable;
import utils.Utilities;

/**
 * {@code LoadClusterIndexFile}연산은 지정된 HDFS 파일 (또는 복수개의 HDFS 파일들)에 저장된
 * 레코드 세트를 로드하는 작업을 수행한다.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LoadSpatialGlobalIndex extends AbstractRecordSetLoader
							implements PBSerializable<LoadSpatialGlobalIndexProto> {
	private final String m_dsId;
	
	public LoadSpatialGlobalIndex(String dsId) {
		Utilities.checkNotNullArgument(dsId, "dsId is null");
		
		m_dsId = dsId;
		setLogger(LoggerFactory.getLogger(LoadSpatialGlobalIndex.class));
	}
	
	@Override
	public void initialize(MarmotCore marmot) {
		setInitialized(marmot, GlobalIndexEntry.SCHEMA);
	}

	@Override
	public RecordSet load() {
		checkInitialized();
		
		return new Loaded(this);
	}
	
	public RecordSet load(MarmotCore marmot) {
		checkInitialized();
		
		return new Loaded(this);
	}
	
	@Override
	public String toString() {
		return String.format("load_cluster_index[dataset=%s]", m_dsId);
	}

	private static class Loaded extends AbstractRecordSet {
		private final LoadSpatialGlobalIndex m_load;
		private final CoordinateTransform m_trans;
		private Iterator<GlobalIndexEntry> m_iter;
		
		public Loaded(LoadSpatialGlobalIndex load) {
			m_load = load;
			
			final MarmotCore marmot = load.getMarmotCore();
			SpatialIndexInfo idxInifo = marmot.getDataSet(load.m_dsId)
												.getSpatialIndexInfo()
												.getOrThrow(() -> IndexNotFoundException.fromDataSet(load.m_dsId));
			HdfsPath dirPath = HdfsPath.of(marmot.getHadoopFileSystem(),
											new Path(idxInifo.getHdfsFilePath()));
			HdfsPath idxFilePath = GlobalIndex.toGlobalIndexPath(dirPath);
			GlobalIndex indexFile = GlobalIndex.open(idxFilePath);
			getLogger().info("loaded ClusterIndexFile, path={}", idxFilePath);
			
			m_iter = indexFile.getIndexEntryAll().iterator();
			
			DataSet ds = marmot.getDataSet(load.m_dsId);
			m_trans = CoordinateTransform.get("EPSG:4326", ds.getGeometryColumnInfo().srid());
		}
	
		@Override
		public RecordSchema getRecordSchema() {
			return m_load.getRecordSchema();
		}
	
		@Override protected void closeInGuard() { }
	
		@Override
		public boolean next(Record record) throws RecordSetException {
			if ( !m_iter.hasNext() ) {
				return false;
			}
			
			GlobalIndexEntry cindex = m_iter.next();
			cindex.copyTo(record);
			record.set("tile_bounds", m_trans.transform((Envelope)record.get("tile_bounds")));
			record.set("data_bounds", m_trans.transform((Envelope)record.get("data_bounds")));
			return true;
		}
		
		@Override
		public String toString() {
			return m_load.toString();
		}
	}

	public static LoadSpatialGlobalIndex fromProto(LoadSpatialGlobalIndexProto proto) {
		return new LoadSpatialGlobalIndex(proto.getDataset());
	}

	@Override
	public LoadSpatialGlobalIndexProto toProto() {
		return LoadSpatialGlobalIndexProto.newBuilder()
										.setDataset(m_dsId)
										.build();
	}
}
