package marmot.module.geo;

import java.util.List;

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
import marmot.proto.process.E2SFCAStep2Proto;
import marmot.support.DefaultRecord;
import marmot.support.PBSerializable;
import marmot.type.DataType;
import utils.CSV;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class E2SFCAStep2 extends NestedLoopSpatialJoin<E2SFCAStep2> implements PBSerializable<E2SFCAStep2Proto> {
	private final List<String> m_inputFeatureCols;
	private final List<String> m_outputFeatureCols;
	private final List<String> m_taggedCols;
	private final double m_radius;
	private final DistanceDecayFunction m_func;
	
	private final List<String> m_ratioCols;
	private final FeatureVectorHandle m_inputFeatureHandle;
	private final FeatureVectorHandle m_ratioHandle;
	private final FeatureVectorHandle m_outHandle;
	
	E2SFCAStep2(String inputGeomCol, String intermDataSet,
				List<String> inputFeatureCols, List<String> outputFeatureCols,
				List<String> taggedCols, double radius, DistanceDecayFunction func) {
		super(inputGeomCol, intermDataSet, SpatialJoinOptions.WITHIN_DISTANCE(radius));

		m_inputFeatureCols = inputFeatureCols;
		m_outputFeatureCols = outputFeatureCols;
		m_taggedCols = taggedCols;
		m_radius = radius;
		m_func = func;

		m_ratioCols = FStream.range(0, m_inputFeatureCols.size())
							.mapToObj(idx -> String.format("r%02d", idx))
							.toList();
		
		m_inputFeatureHandle = new FeatureVectorHandle(m_inputFeatureCols);
		m_ratioHandle = new FeatureVectorHandle(m_ratioCols);
		m_outHandle = new FeatureVectorHandle(m_outputFeatureCols);
	}

	@Override
	protected RecordSchema initialize(MarmotCore marmot, RecordSchema outerSchema,
									RecordSchema paramSchema) {
		RecordSchema.Builder builder = RecordSchema.builder();
		outerSchema.getColumns().stream()
					.filter(col -> !m_inputFeatureCols.contains(col.name()))
					.forEach(col -> builder.addColumn(col.name(), col.type()));
		m_outputFeatureCols.forEach(name -> builder.addOrReplaceColumn(name, DataType.DOUBLE));
		
		m_taggedCols.stream()
				.map(name -> outerSchema.getColumn(name))
				.filter(col -> col != null)
				.forEach(col -> builder.addOrReplaceColumn(col.name(), col.type()));
		
		return builder.build();
	}

	@Override
	protected RecordSet doInnerLoop(NestedLoopMatch match) {
		RecordSet nonEmpty = match.getInnerRecordSet().asNonEmpty().getOrNull();
		if ( nonEmpty == null ) {
			return RecordSet.empty(getOuterRecordSchema());
		}
		
		Geometry outGeom = getOuterGeometry(match.getOuterRecord());
		
		FeatureVector inputFeature = m_inputFeatureHandle.take(match.getOuterRecord());
		double[] sum = new double[inputFeature.length()];
		nonEmpty.forEach(inner -> {
			double distance = outGeom.distance(getInnerGeometry(inner));
			double weight = m_func.apply(distance);
			
			FeatureVector feature2 = m_ratioHandle.take(inner);
			for ( int i =0; i < sum.length; ++i ) {
				sum[i] += (weight * (double)feature2.get(i));
			}
		});
		
		Record output = DefaultRecord.of(m_outputSchema);
		output.set(match.getOuterRecord());
		m_outHandle.put(new FeatureVector(sum), output);
		return RecordSet.of(output);
	}
	
	public static E2SFCAStep2 fromProto(E2SFCAStep2Proto proto) {
		return new E2SFCAStep2(proto.getInputGeomemtryColumn(),
								proto.getIntermediateDataset(),
								CSV.parseCsv(proto.getInputFeatureColumns()).toList(),
								CSV.parseCsv(proto.getOutputFeatureColumns()).toList(),
								CSV.parseCsv(proto.getTaggedColumns()).toList(),
								proto.getRadius(),
								DistanceDecayFunctions.fromString(proto.getDistanceDecayFunction()));
	}
	
	@Override
	public E2SFCAStep2Proto toProto() {
		return E2SFCAStep2Proto.newBuilder()
								.setInputGeomemtryColumn(getOuterGeomColumnName())
								.setIntermediateDataset(getParamDataSetId())
								.setInputFeatureColumns(FStream.from(m_inputFeatureCols).join(","))
								.setOutputFeatureColumns(FStream.from(m_outputFeatureCols).join(","))
								.setTaggedColumns(FStream.from(m_taggedCols).join(","))
								.setRadius(m_radius)
								.setDistanceDecayFunction(m_func.toString())
								.build();
	}
}
