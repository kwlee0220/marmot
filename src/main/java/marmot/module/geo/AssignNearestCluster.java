package marmot.module.geo;

import java.util.List;

import org.apache.hadoop.fs.Path;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.analysis.module.geo.FeatureVector;
import marmot.analysis.module.geo.FeatureVectorHandle;
import marmot.io.HdfsPath;
import marmot.io.MarmotSequenceFile;
import marmot.mapreduce.MarmotMRContext;
import marmot.mapreduce.MarmotMRContexts;
import marmot.optor.rset.SingleInputRecordSet;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.support.DefaultRecord;
import marmot.type.DataType;
import utils.io.IOUtils;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AssignNearestCluster extends AbstractRecordSetFunction {
	private final Path m_workspace;
	private final List<String> m_featureColNames;
	private final String m_clusterColName;
	private final List<FeatureVector> m_centroids;
	
	public AssignNearestCluster(Path workspace, List<String> featureColNames,
								String clusterColName, List<FeatureVector> centroids) {
		m_workspace = workspace;
		m_featureColNames = featureColNames;
		m_clusterColName = clusterColName;
		m_centroids = centroids;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		RecordSchema outSchema = inputSchema.toBuilder()
										.addOrReplaceColumn(m_clusterColName, DataType.INT)
										.build();
		
		setInitialized(marmot, inputSchema, outSchema);
	}

	@Override
	public RecordSet apply(RecordSet input) {
		Path path = m_workspace;
		
		MarmotMRContext context = MarmotMRContexts.getOrNull();
		if ( context != null ) {	// MapReduce를 통해 수행되는 경우
			String name = String.format("%05d", context.getTaskOrdinal());
			path = new Path(m_workspace, name);
		}
		
		return new Assigneds(this, path, input);
	}
	
	static class Assigneds extends SingleInputRecordSet<AssignNearestCluster> {
		private final FeatureVectorHandle m_featureTaker;
		private final List<FeatureVector> m_centroids;
		private final Record m_inputRecord;
		private final int m_clusterColIdx;
		private final MarmotSequenceFile.Writer m_writer;
		
		Assigneds(AssignNearestCluster assign, Path outFile, RecordSet input) {
			super(assign, input);
			
			m_featureTaker = new FeatureVectorHandle(assign.m_featureColNames);
			m_centroids = assign.m_centroids;
			
			RecordSchema inputSchema = input.getRecordSchema();
			RecordSchema outputSchema = assign.getRecordSchema();
			m_clusterColIdx = outputSchema.getColumn(assign.m_clusterColName).ordinal();
			
			m_inputRecord = DefaultRecord.of(inputSchema);
			
			HdfsPath path = HdfsPath.of(assign.m_marmot.getHadoopFileSystem(), outFile);
			m_writer = MarmotSequenceFile.create(path, outputSchema, null);
		}

		@Override
		protected void closeInGuard() {
			IOUtils.closeQuietly(m_writer);
			super.closeInGuard();
		}
		
		@Override
		public boolean next(Record record) {
			checkNotClosed();
			
			if ( !m_input.next(m_inputRecord) ) {
				return false;
			}
			
			FeatureVector feature = m_featureTaker.take(m_inputRecord);
			double minDist = Double.MAX_VALUE;
			int minDistIdx = -1;
			for ( int i =0; i < m_centroids.size(); ++i ) {
				double dist = feature.distance(m_centroids.get(i));
				if ( Double.compare(dist, minDist) < 0 ) {
					minDist = dist;
					minDistIdx = i;
				}
			}
			
			record.set(m_inputRecord);
			record.set(m_clusterColIdx, minDistIdx);
			
			m_writer.write(record);
			
			return true;
		}
	}
}