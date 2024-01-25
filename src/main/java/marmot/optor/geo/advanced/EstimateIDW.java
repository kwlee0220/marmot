package marmot.optor.geo.advanced;

import static java.lang.Math.pow;

import java.util.List;

import org.locationtech.jts.geom.Geometry;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.geo.join.NestedLoopKnnMatch;
import marmot.optor.geo.join.SpatialKnnJoin;
import marmot.plan.SpatialJoinOptions;
import marmot.proto.optor.EstimateIDWProto;
import marmot.support.DataUtils;
import marmot.support.DefaultRecord;
import marmot.support.PBSerializable;
import marmot.type.DataType;
import utils.UnitUtils;
import utils.Utilities;
import utils.func.FOption;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class EstimateIDW extends SpatialKnnJoin<EstimateIDW>
										implements PBSerializable<EstimateIDWProto> {
	private static final double DEFAULT_POWER = 2d;
	
	private final String m_valueColumn;
	private final String m_outColumn;
	private FOption<Double> m_opower;
	
	private int m_valueColIdx;		// set during initialization
	private int m_outColIdx;		// set during initialization
	private double m_power;			// set during initialization

	public EstimateIDW(String geomColumn, String paramDsId, String valueColumn,
						double radius, int topK, String outColumn, FOption<Double> power) {
		super(geomColumn, paramDsId, radius, FOption.of(topK), SpatialJoinOptions.DEFAULT);

		Utilities.checkNotNullArgument(valueColumn, "value column is null");
		Utilities.checkNotNullArgument(outColumn, "density column is null");

		m_valueColumn = valueColumn;
		m_outColumn = outColumn;
		m_opower = power;
		setLogger(LoggerFactory.getLogger(EstimateIDW.class));
	}
	
	public EstimateIDW setPower(double power) {
		Preconditions.checkArgument(power >= 0, "IDW power should be larger than zero");
		Preconditions.checkState(!isInitialized(), "cannot set power after initialization");
		
		m_opower = FOption.of(power);
		return this;
	}

	@Override
	protected RecordSchema initialize(MarmotCore marmot, RecordSchema outerSchema,
										RecordSchema innerSchema) {
		m_valueColIdx = innerSchema.findColumn(m_valueColumn)
								.map(Column::ordinal)
								.getOrThrow(() -> new IllegalArgumentException("invalid value column: name=" + m_valueColumn));

		RecordSchema outSchema = outerSchema.toBuilder()
										.addOrReplaceColumn(m_outColumn, DataType.DOUBLE)
										.build();
		m_outColIdx = outSchema.getColumn(m_outColumn).ordinal();
		
		m_power = m_opower.getOrElse(DEFAULT_POWER);
		
		return outSchema;
	}

	@Override
	protected RecordSet handleKnnMatches(NestedLoopKnnMatch match) {
		Record output = DefaultRecord.of(getRecordSchema());
		output.set(match.getOuterRecord());
		
		Geometry outerGeom = getOuterGeometry(match.getOuterRecord());
		List<Interm> interms = match.getInnerRecords()
									.map(inner -> estimate(outerGeom, inner._1))
									.toList();
		if ( interms.size() > 0 ) {
			Interm accum = FStream.from(interms)
									.reduce((i1, i2) -> i1.add(i2));
			double estimation = accum.m_numerator / accum.m_denominator;
			output.set(m_outColIdx, estimation);
		}
		else {
			output.set(m_outColIdx, 0d);
		}
		
		return RecordSet.of(output);
	}
	
	@Override
	public String toString() {
		return String.format("estimateIDW: value=%s, power=%.2f, output=%s, k=%d, radius=%s",
							m_valueColumn, m_power, m_outColumn, getTopK().get(),
							UnitUtils.toMeterString(getRadius()));
	}
	
	private static class Interm {
		private double m_numerator;
		private double m_denominator;
		
		Interm(double numerator, double denominator) {
			m_numerator = numerator;
			m_denominator = denominator;
		}
		
		Interm add(Interm other) {
			return new Interm(m_numerator+other.m_numerator,
							m_denominator+other.m_denominator);
		}
	}
	
	private Interm estimate(Geometry outerGeom, Record innerRec) {
		Geometry innerGeom = getInnerGeometry(innerRec);

		double measure = DataUtils.asDouble(innerRec.get(m_valueColIdx));
		
		double dist = outerGeom.distance(innerGeom);
		double weight = pow(dist, m_power);
		if ( weight == 0 ) {
			weight = 1;
		}
		weight = 1/weight;
		
		return new Interm(measure*weight, weight);
	}
	
	public static EstimateIDW fromProto(EstimateIDWProto proto) {
		FOption<Double> power = FOption.empty();
		switch ( proto.getOptionalPowerCase() ) {
			case POWER:
				power = FOption.of(proto.getPower());
				break;
			default:
		}
		return new EstimateIDW(proto.getGeomColumn(), proto.getParamDataset(),
								proto.getValueColumn(), proto.getRadius(),
								proto.getTopK(), proto.getOutputDensityColumn(), power);
	}

	@Override
	public EstimateIDWProto toProto() {
		EstimateIDWProto.Builder builder = EstimateIDWProto.newBuilder()
														.setGeomColumn(getOuterGeomColumnName())
														.setParamDataset(getParamDataSetId())
														.setValueColumn(m_valueColumn)
														.setOutputDensityColumn(m_outColumn)
														.setRadius(getRadius())
														.setTopK(getTopK().get());
		m_opower.ifPresent(builder::setPower);
		
		return builder.build();
	}
}