package marmot.optor.geo.advanced;

import static java.lang.Math.PI;
import static java.lang.Math.exp;
import static java.lang.Math.pow;
import static java.lang.Math.sqrt;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.geo.join.NestedLoopKnnMatch;
import marmot.optor.geo.join.SpatialKnnJoin;
import marmot.plan.SpatialJoinOptions;
import marmot.proto.optor.EstimateKernelDensityProto;
import marmot.support.DataUtils;
import marmot.support.DefaultRecord;
import marmot.support.PBSerializable;
import marmot.type.DataType;
import utils.Utilities;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class EstimateKernelDensity extends SpatialKnnJoin<EstimateKernelDensity>
									implements PBSerializable<EstimateKernelDensityProto> {
	private final String m_valueColumn;
	private final String m_densityColumn;
	private final double m_maxRadius;
	
	private int m_valueColIdx;
	private int m_densityColIdx;

	public EstimateKernelDensity(String geomColumn, String dsId, String valueColumn,
								double radius, String densityColumn, SpatialJoinOptions opts) {
		super(geomColumn, dsId, radius, FOption.empty(), opts);
		
		Utilities.checkNotNullArgument(valueColumn, "input value column name");
		Utilities.checkNotNullArgument(densityColumn, "output density column name");
		
		m_valueColumn = valueColumn;
		m_maxRadius = radius;
		m_densityColumn = densityColumn;
	}

	@Override
	protected RecordSchema initialize(MarmotCore marmot, RecordSchema outerSchema,
										RecordSchema innerSchema) {
		m_valueColIdx = innerSchema.findColumn(m_valueColumn)
									.map(Column::ordinal)
									.getOrElse(-1);

		RecordSchema outSchema = outerSchema.toBuilder()
											.addColumn(m_densityColumn, DataType.DOUBLE)
											.build();
		m_densityColIdx = outSchema.getColumn(m_densityColumn).ordinal();
		
		return outSchema;
	}

	@Override
	protected RecordSet handleKnnMatches(NestedLoopKnnMatch match) {
		double avg = match.getInnerRecords()
							.mapToDouble(inner -> calcDensity(inner._1, inner._2))
							.average()
							.getOrElse(0d);
		
		Record output = DefaultRecord.of(getRecordSchema());
		output.set(match.getOuterRecord());
		output.set(m_densityColIdx, avg);
		
		return RecordSet.of(output);
	}
	
	private double calcDensity(Record inner, double dist) {
		double u = dist / m_maxRadius;
		double w = exp(-pow(u,2)/2) / sqrt(2*PI);
		double v = (m_valueColIdx>=0) ? DataUtils.asDouble(inner.get(m_valueColIdx)) : 1;
		
		return w * v;
	}

	public static EstimateKernelDensity fromProto(EstimateKernelDensityProto proto) {
		SpatialJoinOptions opts = SpatialJoinOptions.fromProto(proto.getOptions());
		return new EstimateKernelDensity(proto.getGeomColumn(), proto.getDataset(),
											proto.getValueColumn(), proto.getRadius(),
											proto.getDensityColumn(), opts);
	}

	@Override
	public EstimateKernelDensityProto toProto() {
		return EstimateKernelDensityProto.newBuilder()
										.setGeomColumn(getOuterGeomColumnName())
										.setDataset(getParamDataSetId())
										.setValueColumn(m_valueColumn)
										.setDensityColumn(m_densityColumn)
										.setRadius(m_maxRadius)
										.setOptions(m_options.toProto())
										.build();
	}
}