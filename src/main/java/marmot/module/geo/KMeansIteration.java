package marmot.module.geo;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import marmot.Column;
import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.analysis.module.geo.FeatureVector;
import marmot.io.MultiColumnKey;
import marmot.mapreduce.MarmotMapOutputKeyColumns;
import marmot.optor.MapReduceJoint;
import marmot.optor.Sort;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.proto.optor.KMeansIterationProto;
import marmot.support.PBSerializable;
import marmot.type.DataType;
import utils.CSV;
import utils.io.Serializables;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class KMeansIteration extends AbstractRecordSetFunction
							implements PBSerializable<KMeansIterationProto> {
	private final Path m_workspace;
	private final List<String> m_featureColNames;
	private final String m_clusterColName;	// 클러스터 번호가 기록될 컬럼 이름
	private final List<FeatureVector> m_centroids;
	private final int m_workerCount;
	
	public KMeansIteration(Path workspace, List<String> featureColNames,
							String clusterColName, List<FeatureVector> centroids,
							int workerCount) {
		Preconditions.checkArgument(featureColNames != null && featureColNames.size() > 0,
									"input join columns");
		Preconditions.checkArgument(clusterColName != null && clusterColName.length() > 0,
									"parameter join columns");

		m_workspace = workspace;
		m_featureColNames = featureColNames;
		m_clusterColName = clusterColName;
		m_workerCount = workerCount;
		m_centroids = centroids;
	}

	@Override
	public boolean isStreamable() {
		return false;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		RecordSchema.Builder builder = RecordSchema.builder();
		for ( String colName: m_featureColNames ) {
			Column col = inputSchema.findColumn(colName)
							.getOrThrow(() -> {
								String details = String.format("op=%s: invalid feature column: name=%s",
																this, colName);
								return new IllegalArgumentException(details);
							});
			builder.addColumn(colName, col.type());
		}
		RecordSchema outSchema = builder.addColumn(m_clusterColName, DataType.INT).build();
		
		setInitialized(marmot, inputSchema, outSchema);
	}

	@Override
	public MapReduceJoint getMapReduceJoint(RecordSchema inputSchema) {
		if ( m_marmot == null ) {
			throw new IllegalArgumentException("not initialized: optor=" + this);
		}
		
		MarmotCore marmot = (MarmotCore)m_marmot;
		MultiColumnKey clusterKeyCols = MultiColumnKey.fromString(m_clusterColName) ;
		MarmotMapOutputKeyColumns mokCols = MarmotMapOutputKeyColumns.fromGroupKey(clusterKeyCols);
		
		MapReduceJoint joint = MapReduceJoint.create()
								.setMapOutputKey(mokCols)
								.setReducerCount((m_workerCount > 0)
												? m_workerCount : marmot.getDefaultPartitionCount())
								.addMapper(new AssignNearestCluster(m_workspace, m_featureColNames,
																m_clusterColName, m_centroids))
								.addReducer(new ComputeClusterCenters(m_featureColNames,
																	m_clusterColName),
										Sort.by(m_clusterColName + ":A"));
		if ( joint.getReducerCount() > 1 ) {
			joint.setPartitionerClass(HashPartitioner.class);
		}
		
		return joint;
	}

	@Override
	public RecordSet apply(RecordSet input) {
		AssignNearestCluster assign = new AssignNearestCluster(m_workspace, m_featureColNames,
																m_clusterColName, m_centroids);
		assign.initialize(m_marmot, input.getRecordSchema());
		RecordSchema schema = assign.getRecordSchema();
		
		ComputeClusterCenters compute = new ComputeClusterCenters(m_featureColNames,
																	m_clusterColName);
		compute.initialize(m_marmot, schema);
		schema = compute.getRecordSchema();
		
		Sort sort = Sort.by(m_clusterColName + ":A");
		sort.initialize(m_marmot, schema);
		schema = sort.getRecordSchema();
		
		RecordSet rset = assign.apply(input);
		rset = compute.apply(rset);
		return sort.apply(rset);
	}
	
	@Override
	public String toString() {
		String featureColsStr = m_featureColNames.stream()
												.collect(Collectors.joining(","));
		return String.format("kmeans[{%s}->%s]", featureColsStr, m_clusterColName);
	}

	public static KMeansIteration fromProto(KMeansIterationProto proto) {
		try {
			List<String> featureCols = CSV.parseCsv(proto.getFeatureColumns()).toList();
			FeatureVector[] vects = (FeatureVector[])Serializables.deserialize(proto.getCentroids().toByteArray());
			
			return new KMeansIteration(new Path(proto.getWorkspacePath()), featureCols,
										proto.getClusterColumn(), Arrays.asList(vects),
										proto.getWorkerCount());
		}
		catch ( Exception e ) {
			throw new RecordSetException(e);
		}
	}

	@Override
	public KMeansIterationProto toProto() {
		try {
			String featureCols = FStream.from(m_featureColNames).join(",");
			
			FeatureVector[] vects = m_centroids.toArray(new FeatureVector[m_centroids.size()]);
			byte[] bytes = Serializables.serialize(vects);
			
			return KMeansIterationProto.newBuilder()
										.setWorkspacePath(m_workspace.toString())
										.setFeatureColumns(featureCols)
										.setClusterColumn(m_clusterColName)
										.setCentroids(ByteString.copyFrom(bytes))
										.setWorkerCount(m_workerCount)
								.build();
		}
		catch ( IOException e ) {
			throw new RecordSetException(e);
		}
	}
}
