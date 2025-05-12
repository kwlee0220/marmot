package marmot.exec;

import java.util.List;
import java.util.Map;

import utils.KeyValue;
import utils.Throwables;
import utils.stream.FStream;

import marmot.MarmotCore;
import marmot.analysis.system.SystemAnalysis;
import marmot.dataset.DataSet;
import marmot.geo.command.CreateSpatialIndexOptions;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class SystemAnalysisExecutor {
	private final MarmotCore m_marmot;
	
	SystemAnalysisExecutor(MarmotCore marmot) {
		m_marmot = marmot;
	}

	void execute(SystemAnalysis analysis) throws MarmotExecutionException {
		List<String> args = analysis.getArguments();
		try {
			switch ( analysis.getFunctionId().toLowerCase() ) {
				case "cluster_dataset":
					clusterDataSet(analysis);
					break;
				case "delete_dataset":
					deleteDataSet(args);
					break;
				case "delete_dir":
					deleteDir(args);
					break;
				case "create_thumbnail":
					createThumbnail(args);
					break;
				default:
					throw new IllegalArgumentException("unknown function: id=" + analysis.getFunctionId());
			}
		}
		catch ( Throwable e ) {
			Throwables.throwIfInstanceOf(e, RuntimeException.class);
			throw Throwables.toRuntimeException(e);
		}
	}
	
	private void clusterDataSet(SystemAnalysis anal) {
		List<String> args = anal.getArguments();
		if ( args == null || args.size() < 1 ) {
			throw new IllegalArgumentException("dataset id is missing: " + anal);
		}
		
		String dsId = args.get(0);
		Map<String,String> argMap = FStream.from(args)
											.drop(1)
											.toKeyValueStream(KeyValue::parse)
											.toMap();
		String countStr = argMap.get("workers");
		
		DataSet ds = m_marmot.getDataSet(dsId);
		
		CreateSpatialIndexOptions opts = CreateSpatialIndexOptions.DEFAULT();
		if ( countStr != null ) {
			opts.workerCount(Integer.parseInt(countStr));
		}
		
		ds.createSpatialIndex(opts);
	}
	
	private void deleteDataSet(List<String> args) {
		for ( String dsId: args ) {
			m_marmot.deleteDataSet(dsId);
		}
	}
	
	private void deleteDir(List<String> args) {
		for ( String dir: args ) {
			m_marmot.deleteDir(dir);
		}
	}
	
	private void createThumbnail(List<String> args) {
		String dsId = args.get(0);
		int sampleCount = Integer.parseInt(args.get(1));
		
		DataSet ds = m_marmot.getDataSet(dsId);
		ds.createThumbnail(sampleCount);
	}
}
