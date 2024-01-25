package marmot.optor.geo.advanced;

import static java.lang.Math.pow;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.proto.optor.CalcLocalMoransIProto;
import marmot.proto.optor.LISAParametersProto;
import marmot.proto.optor.LISAWeightProto;
import marmot.support.GeoUtils;
import marmot.support.PBSerializable;
import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CalcLocalMoransI extends LocalSpatialIndicator
						implements PBSerializable<CalcLocalMoransIProto> {
	private static final String COLUMN_MORANI = "moran_i";
	private static final String COLUMN_ZSCORE = "moran_zscore";
	private static final String COLUMN_PVALUE = "moran_pvalue";
	private static final String COLUMN_COTYPE = "cotype";

	private final String m_idColumn;
	private final Map<String,Object> m_params;
	
	private int m_idColIdx;
	
	public CalcLocalMoransI(String geomColumn, String dataset, String idColumn, String valueColumn,
							double radius, LISAWeight weight, Map<String,Object> params) {
		super(geomColumn, dataset, valueColumn, radius, weight);
		
		m_idColumn = idColumn;
		m_params = params;
	}
	
	public static RecordSchema getOutputRecordSchema(RecordSchema dataSchema) {
		return dataSchema.toBuilder()
						.addColumn(COLUMN_MORANI, DataType.DOUBLE)
						.addColumn(COLUMN_ZSCORE, DataType.DOUBLE)
						.addColumn(COLUMN_PVALUE, DataType.DOUBLE)
						.addColumn(COLUMN_COTYPE, DataType.STRING)
						.build();
	}

	@Override
	protected RecordSchema initialize(MarmotCore marmot, RecordSchema inputSchema,
									RecordSchema paramSchema) {
		m_idColIdx = inputSchema.findColumn(m_idColumn)
								.map(Column::ordinal)
								.getOrThrow(() -> new IllegalArgumentException("invalid id column: name=" + m_idColumn));
		
		return getOutputRecordSchema(inputSchema);
	}

	@Override
	protected void calculate(Record input, double xi, List<Neighbor> neighborList, Record output) {
		Object id1 = input.get(m_idColIdx);
		
		Neighbor[] neighbors = neighborList.stream()
								.filter(n -> !id1.equals(n.m_record.get(m_idColIdx)))
								.toArray(sz -> new Neighbor[sz]);
		
		if ( (Integer)id1 == 447 ) {
			System.out.println(neighborList);
		}
		
		long n = (Long)m_params.get("count");
		double avg = (Double)m_params.get("avg");
		double var = pow((Double)m_params.get("stddev"), 2);
		
		double diff = xi - avg;
		
		LocalAggregation local = new LocalAggregation();
		Arrays.stream(neighbors)
				.forEach(neighbor -> {
					local.wsum += neighbor.m_wij;
					local.w2sum += Math.pow(neighbor.m_wij, 2);
					double xdiff = neighbor.m_xj - avg;
					local.xdiff2Sum += Math.pow(xdiff, 2);
					local.xdiff4Sum += Math.pow(xdiff, 4);
					local.wxdiffSum += (neighbor.m_wij * xdiff);
				});
		
		double wcombiSum = 0;
		for ( int i =0; i < neighbors.length; ++i ) {
			for ( int j =0; j < neighbors.length; ++j ) {
				wcombiSum += (neighbors[i].m_wij * neighbors[j].m_wij);
			}
		}
		
		double S2 = ((var * n) - pow(diff,2)) / (n-1);
		double I = (diff / var) * local.wxdiffSum;
		double b2i = local.xdiff4Sum / pow(local.xdiff2Sum,2);
		double A = ((n - b2i) * local.w2sum) / (n-1);
		double B = (((2*b2i) - n) * wcombiSum) / ((n-1) * (n-2));
		double EI2 = A - B;
		double EI = local.wsum / (1-n);
		double VI = EI2 - Math.pow(EI, 2);
		double zscore = (I - EI) / Math.sqrt(VI);
		double pvalue = GeoUtils.toPValue(zscore);
		
		System.out.printf("ID=%d x=%.0f t=%d, n=%d, I=%.5f, EI=%.5f Z=%.5f P=%.5f%n",
							id1, xi, n, neighborList.size(), I, EI, zscore, pvalue);
		
		output.setAll(input.getAll());
		output.set(getValueColumnIndex(), I);
		output.set(getValueColumnIndex()+1, zscore);
		output.set(getValueColumnIndex()+2, pvalue);
		output.set(getValueColumnIndex()+3, ClusterType.fromZValue(xi, zscore, avg).name());
	}
	
	@Override
	public String toString() {
		return "Local Moran's I";
	}

	public static CalcLocalMoransI fromProto(CalcLocalMoransIProto proto) {
		LISAParametersProto paramsProto = proto.getParameters();
		Map<String,Object> params = Maps.newHashMap();
		params.put("count", paramsProto.getCount());
		params.put("avg", paramsProto.getAvg());
		params.put("stddev", paramsProto.getStddev());
		
		return new CalcLocalMoransI(proto.getGeometryColumn(), proto.getDataset(),
								proto.getIdColumn(), proto.getValueColumn(), proto.getRadius(),
								LISAWeight.valueOf(proto.getWeightType().name()), params);
	}

	@Override
	public CalcLocalMoransIProto toProto() {
		return CalcLocalMoransIProto.newBuilder()
								.setDataset(getParamDataSetId())
								.setGeometryColumn(getOuterGeomColumnName())
								.setIdColumn(m_idColumn)
								.setValueColumn(m_valueColumn)
								.setRadius(m_radius)
								.setWeightType(LISAWeightProto.valueOf(getLISAWeight().name()))
								.build();
	}
	
	private static class LocalAggregation {
		double wsum = 0;
		double w2sum = 0;
		double xdiff2Sum = 0;
		double xdiff4Sum = 0;
		double wxdiffSum =0;
	}
}
