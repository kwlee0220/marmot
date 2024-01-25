package marmot.module.geo;

import org.locationtech.jts.geom.Geometry;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSets;
import marmot.optor.geo.advanced.LISAWeight;
import marmot.optor.geo.join.NestedLoopMatch;
import marmot.optor.geo.join.NestedLoopSpatialJoin;
import marmot.plan.SpatialJoinOptions;
import marmot.proto.optor.CalcGlobalMoranIIntermediateProto;
import marmot.proto.optor.LISAWeightProto;
import marmot.support.PBSerializable;
import marmot.type.DataType;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CalcGlobalMoranIIntermediate extends NestedLoopSpatialJoin<CalcGlobalMoranIIntermediate>
								implements PBSerializable<CalcGlobalMoranIIntermediateProto> {
	private final String m_targetColumn;
	private final double m_radius;
	private final LISAWeight m_wtype;
	private final double m_avg;
	
	private int m_targetColIdx;
	
	static class Intermediate {
		double m_var2 = 0;
		double m_var4 = 0;
		double m_weightSum = 0;
		double m_coVarSum = 0;
	}
	
	public CalcGlobalMoranIIntermediate(String dataset, String geomCol, String targetColumn,
										double radius, LISAWeight wtype, double avg) {
		super(geomCol, dataset, SpatialJoinOptions.WITHIN_DISTANCE(radius));
		
		m_targetColumn = targetColumn;
		m_radius = radius;
		m_wtype = wtype;
		m_avg = avg;
	}

	@Override
	protected RecordSchema initialize(MarmotCore marmot, RecordSchema inputSchema,
									RecordSchema paramSchema) {
		RecordSchema outSchema = RecordSchema.builder()
											.addColumn("var2", DataType.DOUBLE)
											.addColumn("var4", DataType.DOUBLE)
											.addColumn("weight_sum", DataType.DOUBLE)
											.addColumn("covar_sum", DataType.DOUBLE)
											.build();
		m_targetColIdx = outSchema.getColumn(m_targetColumn).ordinal();
		
		return outSchema;
	}

	@Override
	protected RecordSet doInnerLoop(NestedLoopMatch match) {
		Intermediate interm = new Intermediate();
		double outerValue = match.getOuterRecord().getDouble(m_targetColIdx);

		Geometry outerGeom = getOuterGeometry(match.getOuterRecord());
		match.getInnerRecords()
			.forEach(innerRec -> {
				Geometry innerGeom = getInnerGeometry(innerRec);
				double w = m_wtype.weigh(outerGeom.distance(innerGeom), m_radius);
				double value = innerRec.getDouble(m_targetColIdx);
				
				interm.m_weightSum += w;
				interm.m_coVarSum += w * (outerValue-m_avg) * (value-m_avg);
			});
		interm.m_var2 = Math.pow(outerValue-m_avg, 2);
		interm.m_var4 = Math.pow(outerValue-m_avg, 4);
		
		return RecordSets.singleton(getOuterRecordSchema(), output -> {
			output.set("var2", interm.m_var2);
			output.set("var4", interm.m_var4);
			output.set("weight_sum", interm.m_weightSum);
			output.set("covar_sum", interm.m_coVarSum);
		});
	}

	public static CalcGlobalMoranIIntermediate fromProto(CalcGlobalMoranIIntermediateProto proto) {
		LISAWeight wtype = LISAWeight.valueOf(proto.getWtype().name());
		return new CalcGlobalMoranIIntermediate(proto.getDataset(),
												proto.getGeometryColumn(),
												proto.getTargetColumn(),
												proto.getRadius(),
												wtype, proto.getAverage());
	}

	@Override
	public CalcGlobalMoranIIntermediateProto toProto() {
		return CalcGlobalMoranIIntermediateProto.newBuilder()
								.setDataset(getParamDataSetId())
								.setGeometryColumn(getOuterGeomColumnName())
								.setTargetColumn(m_targetColumn)
								.setRadius(m_radius)
								.setWtype(LISAWeightProto.valueOf(m_wtype.name()))
								.setAverage(m_avg)
								.build();
	}
}