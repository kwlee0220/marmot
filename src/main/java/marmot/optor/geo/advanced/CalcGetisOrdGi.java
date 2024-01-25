package marmot.optor.geo.advanced;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.proto.optor.CalcGetisOrdGiProto;
import marmot.proto.optor.LISAParametersProto;
import marmot.proto.optor.LISAWeightProto;
import marmot.support.GeoUtils;
import marmot.support.PBSerializable;
import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CalcGetisOrdGi extends LocalSpatialIndicator
						implements PBSerializable<CalcGetisOrdGiProto>  {
	private static final String COLUMN_ZSCORE = "gi_zscore";
	private static final String COLUMN_PVALUE = "gi_pvalue";
	
	private final Map<String,Object> m_params;

	public CalcGetisOrdGi(String geomColumn, String dataset, String valueColumn,
						double radius, LISAWeight weight, Map<String,Object> params) {
		super(geomColumn, dataset, valueColumn, radius, weight);
		
		m_params = params;
	}
	
	public CalcGetisOrdGi(String geomColumn, String dataset, String valueColumn,
							double radius, Map<String,Object> params) {
		this(geomColumn, dataset, valueColumn, radius, LISAWeight.FIXED_DISTANCE_BAND, params);
	}

	public static RecordSchema getOutputRecordSchema(RecordSchema dataSchema) {
		return dataSchema.toBuilder()
						.addColumn(COLUMN_ZSCORE, DataType.DOUBLE)
						.addColumn(COLUMN_PVALUE, DataType.DOUBLE)
						.build();
	}

	@Override
	protected RecordSchema initialize(MarmotCore marmot, RecordSchema inputSchema,
									RecordSchema paramSchema) {
		return getOutputRecordSchema(inputSchema);
	}

	@Override
	protected void calculate(Record input, double xi, List<Neighbor> neighbors, Record output) {
		long n = (Long)m_params.get("count");
		double avg = (Double)m_params.get("avg");
		double stddev = (Double)m_params.get("stddev");
		
		LocalAggregation local = new LocalAggregation();
		neighbors.stream()
				.forEach(neighbor -> {
					local.wsum += neighbor.m_wij;
					local.w2sum += (neighbor.m_wij * neighbor.m_wij);
					local.wxsum += (neighbor.m_wij * neighbor.m_xj);
				});
		double wSumSquared = local.wsum * local.wsum;
		
		double numerator = local.wxsum - (avg*local.wsum);
		double denum = (n*local.w2sum) - wSumSquared;
		denum = stddev * Math.sqrt(denum / (n - 1));
		double zvalue = numerator / denum;
		double pvalue = GeoUtils.toPValue(zvalue);
		
		output.setAll(input.getAll());
		output.set(getValueColumnIndex(), zvalue);
		output.set(getValueColumnIndex()+1, pvalue);
	}
	
	@Override
	public String toString() {
		return "Getis-Ord Gi*";
	}

	public static CalcGetisOrdGi fromProto(CalcGetisOrdGiProto proto) {
		LISAParametersProto paramsProto = proto.getParameters();
		Map<String,Object> params = Maps.newHashMap();
		params.put("count", paramsProto.getCount());
		params.put("avg", paramsProto.getAvg());
		params.put("stddev", paramsProto.getStddev());
		
		return new CalcGetisOrdGi(proto.getGeometryColumn(), proto.getDataset(),
								proto.getValueColumn(), proto.getRadius(),
								LISAWeight.valueOf(proto.getWeightType().name()), params);
	}

	@Override
	public CalcGetisOrdGiProto toProto() {
		return CalcGetisOrdGiProto.newBuilder()
								.setDataset(getParamDataSetId())
								.setGeometryColumn(getOuterGeomColumnName())
								.setValueColumn(m_valueColumn)
								.setRadius(m_radius)
								.setWeightType(LISAWeightProto.valueOf(getLISAWeight().name()))
								.build();
	}
	
	private static class LocalAggregation {
		double wsum =0;
		double w2sum =0;
		double wxsum =0;
	}
}
