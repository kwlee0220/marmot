package marmot.module.geo;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.google.protobuf.ByteString;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSetException;
import marmot.analysis.module.geo.FeatureVector;
import marmot.analysis.module.geo.FeatureVectorHandle;
import marmot.optor.support.RecordLevelTransform;
import marmot.proto.optor.AssignClusterIdProto;
import marmot.support.PBSerializable;
import marmot.type.DataType;
import utils.CSV;
import utils.io.Serializables;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class AssignClusterId extends RecordLevelTransform
					implements PBSerializable<AssignClusterIdProto> {
	private final List<String> m_featureColNames;
	private final String m_clusterColName;
	private final List<FeatureVector> m_centroids;
	
	private FeatureVectorHandle m_taker;
	private int m_clusterColIdx = -1;

	AssignClusterId(List<String> featureColNames, String clusterColName,
					List<FeatureVector> centroids) {
		m_featureColNames = featureColNames;
		m_clusterColName = clusterColName;
		m_centroids = centroids;
		m_taker = new FeatureVectorHandle(featureColNames);
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		RecordSchema outSchema = inputSchema.toBuilder()
										.addColumn(m_clusterColName, DataType.INT)
										.build();
		m_clusterColIdx = outSchema.getColumn(m_clusterColName).ordinal();
		
		setInitialized(marmot, inputSchema, outSchema);
	}

	@Override
	public boolean transform(Record input, Record output) {
		FeatureVector feature = m_taker.take(input);
		
		double minDist = Double.MAX_VALUE;
		int minDistIdx = -1;
		for ( int i =0; i < m_centroids.size(); ++i ) {
			double dist = feature.distance(m_centroids.get(i));
			if ( Double.compare(dist, minDist) < 0 ) {
				minDist = dist;
				minDistIdx = i;
			}
		}
		
		output.set(input);
		output.set(m_clusterColIdx, minDistIdx);
		
		return true;
	}

	public static AssignClusterId fromProto(AssignClusterIdProto proto) {
		try {
			List<String> featureCols = CSV.parseCsv(proto.getFeatureColumns()).toList();
			FeatureVector[] vects = (FeatureVector[])Serializables.deserialize(proto.getCentroids().toByteArray());
			
			return new AssignClusterId(featureCols, proto.getClusterColumn(),
										Arrays.asList(vects));
		}
		catch ( Exception e ) {
			throw new RecordSetException(e);
		}
	}

	@Override
	public AssignClusterIdProto toProto() {
		try {
			String featureCols = FStream.from(m_featureColNames).join(",");
			
			FeatureVector[] vects = m_centroids.toArray(new FeatureVector[m_centroids.size()]);
			byte[] bytes = Serializables.serialize(vects);
			
			return AssignClusterIdProto.newBuilder()
								.setFeatureColumns(featureCols)
								.setClusterColumn(m_clusterColName)
								.setCentroids(ByteString.copyFrom(bytes))
								.build();
		}
		catch ( IOException e ) {
			throw new RecordSetException(e);
		}
	}
}