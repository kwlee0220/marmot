package marmot.optor;

import java.util.List;

import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.dataset.DataSet;
import marmot.dataset.DataSetType;
import marmot.geowave.LoadGHdfsFile;
import marmot.optor.geo.cluster.LoadSpatialClusterFile;
import marmot.optor.support.LoadEmptyMarmotFile;
import marmot.plan.LoadOptions;
import marmot.proto.optor.LoadDataSetProto;
import marmot.support.PBSerializable;
import marmot.support.RecordSetOperatorChain;
import utils.CSV;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LoadDataSet extends CompositeRecordSetLoader
						implements PBSerializable<LoadDataSetProto> {
	private final List<String> m_dsIdList;
	private final LoadOptions m_options;
	private DataSet m_first;
	
	public LoadDataSet(String dsId, LoadOptions opts) {
		m_dsIdList = Lists.newArrayList(dsId);
		m_options = opts;
	}
	
	public LoadDataSet(List<String> dsIdList, LoadOptions opts) {
		m_dsIdList = Lists.newArrayList(dsIdList);
		m_options = opts;
	}

	@Override
	protected RecordSchema _initialize(MarmotCore marmot) {
		m_first = marmot.getDataSet(m_dsIdList.get(0));
		RecordSchema ref = m_first.getRecordSchema();
		
		String diffDsId = FStream.from(m_dsIdList)
								.drop(1)
								.filter(dsId -> !ref.equals(marmot.getDataSet(dsId).getRecordSchema()))
								.findFirst()
								.getOrNull();
		if ( diffDsId != null ) {
			String msg = String.format("incompatible RecordSchema: %s <-> %s", m_dsIdList, diffDsId);
			throw new IllegalArgumentException(msg);
		}
		
		return ref;
	}

	@Override
	protected RecordSetOperatorChain createComponents() {
		checkInitialized();
		
		DataSetType type = m_first.getType();
		String path = m_first.getHdfsPath();
		
		RecordSetLoader loader;
		if ( DataSetType.FILE.equals(type) || DataSetType.LINK.equals(type) ) {
			// DataSet이 empty인 경우는 empty RecordSet을 반환하도록 한다.
			if ( !m_marmot.getFileServer().existsMarmotFile(path) ) {
				loader = new LoadEmptyMarmotFile(m_first.getRecordSchema());
			}
			else {
				List<String> pathList = FStream.from(m_dsIdList)
												.map(m_marmot::getDataSet)
												.map(DataSet::getHdfsPath)
												.toList();
				
				loader = new LoadMarmotFile(pathList, m_options);
			}
		}
		else if ( DataSetType.TEXT.equals(type) ) {
			if ( m_dsIdList.size() > 1 ) {
				throw new IllegalArgumentException("Cannot load multiple source Text DataSet: "
													+ m_dsIdList);
			}
			
			loader = new LoadCustomTextFile(path, m_options);
		}
		else if ( DataSetType.SPATIAL_CLUSTER.equals(type) ) {
			if ( m_dsIdList.size() > 1 ) {
				throw new IllegalArgumentException("Cannot load multiple source SpatialCluster DataSet: "
													+ m_dsIdList);
			}

			loader = new LoadSpatialClusterFile(new Path(path)).setOptions(m_options);
		}
		else if ( type == DataSetType.GWAVE ) {
			String layerName = CSV.parseCsv(path, '/').findLast().get();
			loader = LoadGHdfsFile.from(layerName);
		}
		else {
			throw new RecordSetException("unsupported DataSetType: type=" + type);
		}

		return RecordSetOperatorChain.from(m_marmot).add(loader);
	}
	
	public static RecordSet load(MarmotCore marmot, String dsId) {
		LoadDataSet load = new LoadDataSet(dsId, LoadOptions.DEFAULT);
		load.initialize(marmot);
		return load.load();
	}
	
	@Override
	public String toString() {
		String dsIdStr = m_dsIdList.size() > 1 ? m_dsIdList.toString() : m_dsIdList.get(0);
		String nsplitStr = m_options.splitCount()
									.map(cnt -> String.format("(%d)", cnt))
									.getOrElse("");
		return String.format("load_dataset[%s%s]", dsIdStr, nsplitStr);
	}

	public static LoadDataSet fromProto(LoadDataSetProto proto) {
		LoadOptions opts = LoadOptions.fromProto(proto.getOptions());
		return new LoadDataSet(proto.getDsIdsList(), opts);
	}

	@Override
	public LoadDataSetProto toProto() {
		return LoadDataSetProto.newBuilder()
								.addAllDsIds(m_dsIdList)
								.setOptions(m_options.toProto())
								.build();
	}
}
