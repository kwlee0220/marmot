package marmot.module.geo;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.locationtech.jts.geom.Geometry;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.analysis.module.geo.DistanceDecayFunction;
import marmot.analysis.module.geo.DistanceDecayFunctions;
import marmot.analysis.module.geo.FeatureVector;
import marmot.analysis.module.geo.FeatureVectorHandle;
import marmot.optor.geo.join.NestedLoopMatch;
import marmot.optor.geo.join.NestedLoopSpatialJoin;
import marmot.plan.SpatialJoinOptions;
import marmot.proto.process.E2SFCAStep1Proto;
import marmot.support.DataUtils;
import marmot.support.DefaultRecord;
import marmot.support.PBSerializable;
import marmot.type.DataType;
import utils.CSV;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class E2SFCAStep1 extends NestedLoopSpatialJoin<E2SFCAStep1> implements PBSerializable<E2SFCAStep1Proto> {
	private final List<String> m_inputFeatureCols;
	private final List<String> m_paramFeatureCols;
	private final double m_radius;
	private final DistanceDecayFunction m_func;
	private final List<String> m_ratioCols;
	private final FeatureVectorHandle m_inputFeatureHandle;
	private final FeatureVectorHandle m_paramFeatureHandle;
	private final FeatureVectorHandle m_outFeaturesHandle;
	
	E2SFCAStep1(String paramGeomCol, String inputDataSet,
				List<String> inputFeatureCols, List<String> paramFeatureCols,
				double radius, DistanceDecayFunction func) {
		super(paramGeomCol, inputDataSet, SpatialJoinOptions.WITHIN_DISTANCE(radius));
		
		m_inputFeatureCols = inputFeatureCols;
		m_paramFeatureCols = paramFeatureCols;
		m_radius = radius;
		m_func = func;

		m_ratioCols = IntStream.range(0, m_inputFeatureCols.size())
								.mapToObj(idx -> String.format("r%02d", idx))
								.collect(Collectors.toList());
		
		m_inputFeatureHandle = new FeatureVectorHandle(m_inputFeatureCols);
		m_paramFeatureHandle = new FeatureVectorHandle(m_paramFeatureCols);
		m_outFeaturesHandle = new FeatureVectorHandle(m_ratioCols);
	}

	@Override
	protected RecordSchema initialize(MarmotCore marmot, RecordSchema outerSchema,
									RecordSchema paramSchema) {
		RecordSchema.Builder builder = outerSchema.complement(m_paramFeatureCols)
													.toBuilder();
		m_ratioCols.forEach(name -> builder.addOrReplaceColumn(name, DataType.DOUBLE));
		
		return builder.build();
	}

	@Override
	protected RecordSet doInnerLoop(NestedLoopMatch match) {
		RecordSet nonEmpty = match.getInnerRecordSet().asNonEmpty().getOrNull();
		if ( nonEmpty == null ) {
			return RecordSet.empty(getOuterRecordSchema());
		}
		
		Geometry outGeom = getOuterGeometry(match.getOuterRecord());
		
		FeatureVector paramFeature = m_paramFeatureHandle.take(match.getOuterRecord());
		double[] sum = new double[paramFeature.length()];
		nonEmpty.forEach(inner -> {
			double distance = outGeom.distance(getInnerGeometry(inner));
			double weight = m_func.apply(distance);
			
			FeatureVector inputFeature = m_inputFeatureHandle.take(inner);
			for ( int i =0; i < sum.length; ++i ) {
				sum[i] += (weight * (double)inputFeature.get(i));
			}
		});
		double[] result = new double[paramFeature.length()];
		for ( int i =0; i < sum.length; ++i ) {
			result[i] = DataUtils.asDouble(paramFeature.get(i)) / sum[i];
		}
		
		Record output = DefaultRecord.of(m_outputSchema);
		output.set(match.getOuterRecord());
		m_outFeaturesHandle.put(new FeatureVector(result), output);
		return RecordSet.of(output);
	}
	
	@Override
	public String toString() {
		return String.format("E2SFCA[step1:dataset=%s]", getParamDataSetId(), getJoinExpr());
	}
	
	public static E2SFCAStep1 fromProto(E2SFCAStep1Proto proto) {
		return new E2SFCAStep1(proto.getParamGeomemtryColumn(),
							proto.getInputDataset(),
							CSV.parseCsv(proto.getInputFeatureColumns()).toList(),
							CSV.parseCsv(proto.getParamFeatureColumns()).toList(),
							proto.getRadius(),
							DistanceDecayFunctions.fromString(proto.getDistanceDecayFunction()));
	}
	
	@Override
	public E2SFCAStep1Proto toProto() {
		return E2SFCAStep1Proto.newBuilder()
								.setParamGeomemtryColumn(getOuterGeomColumnName())
								.setInputDataset(getParamDataSetId())
								.setInputFeatureColumns(FStream.from(m_inputFeatureCols).join(","))
								.setParamFeatureColumns(FStream.from(m_paramFeatureCols).join(","))
								.setRadius(m_radius)
								.setDistanceDecayFunction(m_func.toString())
								.build();
	}
}
