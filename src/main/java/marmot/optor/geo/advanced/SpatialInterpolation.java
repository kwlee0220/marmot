package marmot.optor.geo.advanced;

import java.util.List;

import javax.annotation.Nullable;

import org.locationtech.jts.geom.Geometry;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.geo.advanced.InterpolationMethod.SpatialFactor;
import marmot.optor.geo.join.NestedLoopKnnMatch;
import marmot.optor.geo.join.SpatialKnnJoin;
import marmot.plan.SpatialJoinOptions;
import marmot.proto.optor.InterpolateSpatiallyProto;
import marmot.support.DataUtils;
import marmot.support.DefaultRecord;
import marmot.support.PBSerializable;
import marmot.type.DataType;
import utils.CSV;
import utils.UnitUtils;
import utils.Utilities;
import utils.func.FOption;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialInterpolation extends SpatialKnnJoin<SpatialInterpolation>
										implements PBSerializable<InterpolateSpatiallyProto> {
	private final String m_valueColumns;
	private final String m_outColumns;
	@Nullable private final String m_methodId;		// m_methodId 또는 m_function은 null이 아님
	
	private int[] m_valueColIdxes;		// set during initialization
	private int[] m_outColIdxes;		// set during initialization
	@Nullable private InterpolationMethod m_method;	// m_method 또는 m_interpolator은 null이 아님
	@Nullable private UserDefinedInterpolation m_interpolator;

	public SpatialInterpolation(String geomColumn, String paramDsId, String valueColumns,
								double radius, FOption<Integer> topK, String outColumns,
								String methodId) {
		super(geomColumn, paramDsId, radius, topK, SpatialJoinOptions.DEFAULT);

		Utilities.checkNotNullArgument(valueColumns, "value columns");
		Utilities.checkNotNullArgument(outColumns, "output columns");
		Utilities.checkNotNullArgument(methodId, "interpolation method");

		m_valueColumns = valueColumns;
		m_outColumns = outColumns;
		m_methodId = methodId;
		setLogger(LoggerFactory.getLogger(SpatialInterpolation.class));
	}

	@Override
	protected RecordSchema initialize(MarmotCore marmot, RecordSchema outerSchema,
										RecordSchema innerSchema) {
		List<Integer> colIdxes = Lists.newArrayList();
		for ( String colName: CSV.parseCsv(m_valueColumns, ',', '\\').toList() ) {
			innerSchema.findColumn(colName)
						.ifPresent(col -> colIdxes.add(col.ordinal()))
						.orThrow(() -> new IllegalArgumentException("invalid value column: name=" + colName));
		}
		m_valueColIdxes = Ints.toArray(colIdxes);
		
		RecordSchema outSchema = CSV.parseCsv(m_outColumns, ',', '\\')
									.fold(outerSchema.toBuilder(),
												(b,n) -> b.addOrReplaceColumn(n, DataType.DOUBLE))
									.build();
		m_outColIdxes = CSV.parseCsv(m_outColumns, ',', '\\')
							.map(outSchema::getColumn)
							.mapToInt(Column::ordinal)
							.toArray();
		if ( m_valueColIdxes.length != m_outColIdxes.length ) {
			throw new IllegalArgumentException("number of value columns does not match "
												+ "to that of output columns");
		}

		m_method = InterpolationMethod.fromString(m_methodId);
		
		return outSchema;
	}

	@Override
	protected RecordSet handleKnnMatches(NestedLoopKnnMatch match) {
		Record output = DefaultRecord.of(getRecordSchema());
		output.set(match.getOuterRecord());

		Geometry outerGeom = getOuterGeometry(match.getOuterRecord());
		List<SpatialFactor[]> factorVects = match.getInnerRecords()
												.map(inner -> getPortion(outerGeom, inner._1))
												.toList();
		for ( int i =0; i < m_valueColIdxes.length; ++i ) {
			int idx = i;
			List<SpatialFactor> factors = FStream.from(factorVects)
												.map(vect -> vect[idx])
												.toList();
			double estimate = m_method.interpolate(factors);
			output.set(m_outColIdxes[i], estimate);
		}
		
		return RecordSet.of(output);
	}
	
	@Override
	public String toString() {
		String topKStr = getTopK().map(k -> String.format(", k=%d", k))
									.getOrElse("");
		return String.format("interpolate_spatially: value=%s, output=%s, radius=%s%s",
							m_valueColumns, m_outColumns,
							UnitUtils.toMeterString(getRadius()), topKStr);
	}
	
	private SpatialFactor[] getPortion(Geometry outerGeom, Record innerRec) {
		Geometry innerGeom = getInnerGeometry(innerRec);

		double dist = outerGeom.distance(innerGeom);
		return FStream.of(m_valueColIdxes)
						.map(idx -> DataUtils.asDouble(innerRec.get(idx)))
						.map(v -> new SpatialFactor(v, dist))
						.toArray(SpatialFactor.class);
	}
	
	public static SpatialInterpolation fromProto(InterpolateSpatiallyProto proto) {
		FOption<Integer> topK = FOption.empty();
		switch ( proto.getOptionalTopKCase() ) {
			case TOP_K:
				FOption.of(proto.getTopK());
				break;
			case OPTIONALTOPK_NOT_SET:
				break;
			default:
				throw new AssertionError();
		}
		
		return new SpatialInterpolation(proto.getGeomColumn(), proto.getParamDataset(),
											proto.getValueColumns(), proto.getRadius(), topK,
											proto.getOutputColumns(),
											proto.getInterpolationMethod());
	}

	@Override
	public InterpolateSpatiallyProto toProto() {
		InterpolateSpatiallyProto.Builder builder
				= InterpolateSpatiallyProto.newBuilder()
											.setGeomColumn(getOuterGeomColumnName())
											.setParamDataset(getParamDataSetId())
											.setValueColumns(m_valueColumns)
											.setOutputColumns(m_outColumns)
											.setRadius(getRadius())
											.setInterpolationMethod(m_methodId);
		getTopK().ifPresent(builder::setTopK);
		
		return builder.build();
	}
}