package marmot.module.geo;

import static marmot.optor.StoreDataSetOptions.FORCE;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import marmot.MarmotCore;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.analysis.module.geo.FeatureVector;
import marmot.analysis.module.geo.FeatureVectorHandle;
import marmot.analysis.module.geo.KMeansParameters;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.catalog.DataSetInfo;
import marmot.io.HdfsPath;
import marmot.module.MarmotModule;
import marmot.support.DefaultRecord;
import marmot.type.DataType;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ClusterWithKMeans implements MarmotModule {
	private static final Logger s_logger = LoggerFactory.getLogger(ClusterWithKMeans.class);

	private MarmotCore m_marmot;
	private KMeansParameters m_params;
	private FeatureVectorHandle m_handle;

	@Override
	public List<String> getParameterNameAll() {
		return KMeansParameters.getParameterNameAll();
	}
	
	@Override
	public RecordSchema getOutputRecordSchema(MarmotCore marmot, Map<String, String> paramsMap) {
		KMeansParameters params = KMeansParameters.fromMap(paramsMap);
		DataSet input = marmot.getDataSet(params.inputDataset());
		return input.getRecordSchema()
					.toBuilder()
					.addOrReplaceColumn(params.clusterColumn(), DataType.INT)
					.build();
	}

	@Override
	public void initialize(MarmotCore marmot, Map<String, String> paramsMap) {
		m_marmot = marmot;
		m_params = KMeansParameters.fromMap(paramsMap);
	}

	@Override
	public void run() {
		m_handle = new FeatureVectorHandle(m_params.featureColumns());
		
		DataSetInfo info = m_marmot.getDataSetInfo(m_params.inputDataset());
		RecordSchema schema = info.getRecordSchema();
		
		List<String> msgs = FeatureVector.validate(schema, m_params.featureColumns());
		if ( !msgs.isEmpty() ) {
			String details = msgs.stream().collect(Collectors.joining("\n"));
			throw new IllegalArgumentException(details);
		}
		
		FileSystem fs = m_marmot.getHadoopFileSystem();
		Path top = new Path(new Path("tmp/kmeans/"), UUID.randomUUID().toString());

		final double termDist = m_params.terminationDistance();
		final int termIters = m_params.terminationIteration();
		final List<FeatureVector> centroids = Lists.newArrayList(m_params.initialCentroids());
		double maxDist = Double.MAX_VALUE;
		try {
			for ( int i = 0; i < termIters; ++i ) {
				if ( Double.compare(maxDist, termDist) <= 0 ) {
					break;
				}
				
				List<FeatureVector> updateds = adjustCentroids(m_marmot, centroids, top, i);
				maxDist = IntStream.range(0, centroids.size())
									.mapToDouble(idx -> centroids.get(idx)
																.distance(updateds.get(idx)))
									.max()
									.getAsDouble();
				Collections.copy(centroids, updateds);
				
				if ( i >= 2 ) {
					HdfsPath.of(m_marmot.getHadoopConfiguration(), toOutputFile(top, i-2)).delete();
				}
		
				if ( s_logger.isInfoEnabled() ) {
					String tail = "";
					
					if ( termDist > 0 && termIters > 0 ) {
						tail = String.format("term:{iters=%d,dist=%.1f}", termIters, termDist);
					}
					if ( termDist > 0 ) {
						tail = String.format("term.dist=%.1f", termDist);
					}
					if ( termIters > 0 ) {
						tail = String.format("term.iters=%d", termIters);
					}
					s_logger.info(String.format("iter=%03d: max distance=%.1f,%s",
												i, maxDist, tail));
				}
			}
			
			AssignClusterId assign = new AssignClusterId(m_params.featureColumns(),
															m_params.clusterColumn(), centroids);

			GeometryColumnInfo gcInfo = info.getGeometryColumnInfo().get();
			Plan plan = Plan.builder("assign_cluster_id")
								.load(m_params.inputDataset())
								.add(assign)
								.store(m_params.outputDataset(), FORCE(gcInfo))
								.build();
			m_marmot.execute(plan);
		}
		finally {
			HdfsPath.of(m_marmot.getHadoopConfiguration(), top).delete();
		}
	}
	
	private List<FeatureVector> adjustCentroids(MarmotCore marmot,
												List<FeatureVector> centroids,
												Path top, int i) {
		Path outputFile = toOutputFile(top, i);
		String jobName = String.format("kmeans_iter%03d", i);
		Plan plan;
		if ( i > 0 ) {
			String inputFile = toOutputFile(top, i-1).toString(); 
			plan = buildIterationPlan(jobName, inputFile, centroids, outputFile);
		}
		else {
			plan = buildInitIterationPlan(centroids, outputFile);
		}
		
		List<FeatureVector> adjusteds = Lists.newArrayList(centroids);
		try ( RecordSet rset = marmot.executeToRecordSet(plan) ) {
			rset.forEach(r -> {
				int grp = r.getInt(m_params.clusterColumn());
				adjusteds.set(grp, m_handle.take(r));
			});
		}
		return adjusteds;
	}
	
	@Override
	public String toString() {
		return String.format("kmeans[input=%s,feature={%s},cluster=%s,output=%s]",
								m_params.inputDataset(), m_params.initialCentroids().size(),
								m_params.featureColumns(), m_params.clusterColumn());
	}
	
	public static List<FeatureVector> pickInitialCentroids(MarmotRuntime marmot, String dsId,
														List<String> featureColNames, int count)
		throws IOException {
		DataSet ds = marmot.getDataSet(dsId);
		
		FeatureVectorHandle taker = new FeatureVectorHandle(featureColNames);
		Record record = DefaultRecord.of(ds.getRecordSchema());
		
		List<FeatureVector> centroids = Lists.newArrayList();
		try ( RecordSet rset = ds.read() ) {
			while ( rset.next(record) ) {
				FeatureVector feature = taker.take(record);
				
				boolean hasDuplicate = centroids.stream()
										.mapToDouble(c -> feature.distance(c))
										.anyMatch(dist -> Double.compare(dist, 0d) == 0);
				if ( !hasDuplicate ) {
					centroids.add(feature);
					if ( centroids.size() >= count ) {
						return centroids;
					}
				}
			}
			
			return centroids;
		}
	}
	
	private Plan buildInitIterationPlan(List<FeatureVector> centroids, Path output) {
		KMeansIteration iter = new KMeansIteration(output, m_params.featureColumns(),
													m_params.clusterColumn(), centroids, 1);
		String colsExpr = m_params.featureColumns()
									.stream()
									.collect(Collectors.joining(","));
		return Plan.builder("init_iteration")
						.load(m_params.inputDataset())
						.project(colsExpr)
						.add(iter)
						.build();
	}
	
	private Plan buildIterationPlan(String name, String input, List<FeatureVector> centroids,
									Path output) {
		KMeansIteration iter = new KMeansIteration(output, m_params.featureColumns(),
													m_params.clusterColumn(), centroids, 1);
		
		return Plan.builder(name)
						.loadMarmotFile(input)
						.add(iter)
						.build();
	}
	
	private Path toOutputFile(Path workspace, int idx) {
		return new Path(workspace, String.format("output_%03d", idx));
	}
}
