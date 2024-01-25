package marmot.module.geo;


import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.SUM;

import java.util.UUID;
import java.util.function.Consumer;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;

import marmot.MarmotCore;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.dataset.DataSet;
import marmot.optor.LoadMarmotFile;
import marmot.optor.StoreAsHeapfile;
import marmot.optor.geo.advanced.LISAWeight;
import marmot.support.DefaultRecord;
import marmot.type.DataType;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindGlobalMoranI implements Consumer<MarmotCore> {
	private final String m_dataset;
	private final String m_targetColumn;
	private final double m_radius;
	private final LISAWeight m_wtype;
	private final String m_outputFilePath;
	
	private FindGlobalMoranI(String dataset, String targetColumn, double radius,
							LISAWeight weightType, String outputFilePath) {
		Preconditions.checkArgument(dataset != null, "input dataset name should not be null");
		Preconditions.checkArgument(targetColumn != null, "target column name should not be null");
		Preconditions.checkArgument(radius >= 0, "radius must be larger or equal to zero");
		Preconditions.checkArgument(weightType != null, "LISAWeight should not be null");
		Preconditions.checkArgument(outputFilePath != null, "output file path should not be null");
		
		m_dataset = dataset;
		m_targetColumn = targetColumn;
		m_radius = radius;
		m_wtype = weightType;
		m_outputFilePath = outputFilePath;
	}

	@Override
	public void accept(MarmotCore marmot) {
		Path tempPath = new Path("tmp/" + UUID.randomUUID());
		Plan plan = Plan.builder("find_stddev")
								.load(m_dataset)
								.aggregate(COUNT(), AVG(m_targetColumn))
								.storeMarmotFile(tempPath.toString())
								.build();
		marmot.execute(plan);
		
		Record result = LoadMarmotFile.readARecord(marmot, tempPath);
		long count = result.getLong("count");
		double avg = result.getDouble("avg");
		marmot.getFileServer().deleteFile(tempPath);
		
		DataSet dataset = marmot.getDataSet(m_dataset);
		
		CalcGlobalMoranIIntermediate calc
			= new CalcGlobalMoranIIntermediate(m_dataset,
												dataset.getGeometryColumnInfo().name(),
												m_targetColumn, m_radius, m_wtype, avg);
		
		plan = Plan.builder("calc_global_moranI")
					.load(m_dataset)
					.add(calc)
					.defineColumn("weight_sum_squared:double", "weight_sum*weight_sum")
					.aggregate(SUM("covar_sum").as("covar_sum"),
								SUM("var2").as("var2_sum"),
								SUM("var4").as("var4_sum"),
								SUM("weight_sum").as("S0"),
								SUM("weight_sum_squared").as("S2"))
					.storeMarmotFile(tempPath.toString())
					.build();
		marmot.execute(plan);
		
		result = LoadMarmotFile.readARecord(marmot, tempPath);
		double covarSum = result.getDouble("covar_sum");
		double var2Sum = result.getDouble("var2_sum");
		double var4Sum = result.getDouble("var4_sum");
		double S0 = result.getDouble("S0");
		double S2 = result.getDouble("S2");
		marmot.getFileServer().deleteFile(tempPath);
		
		double n = count;
		double n2 = Math.pow(count, 2);
		double S02 = Math.pow(S0, 2);
		double S1 = S2/2;
		double A = n * ((n2 - 3*n +3)*S1 + n*S2 + 3*S02);
		double C = (n-1) * (n-2) * (n-3) * S02;
		double D = var4Sum / Math.pow(var2Sum, 2);
		double B = D * ((n2-n)*S1 - 2*n*S2 + 6*S02);
		
		double I = (n * covarSum) / (S0*var2Sum);
		double EI = -1 / (n - 1);
		double I2Expected = (A - B) / C;
		double VI = I2Expected - Math.pow(EI, 2);
		double zscore = (I - EI) / Math.sqrt(VI);
		
		RecordSchema outputSchema = RecordSchema.builder()
												.addColumn("moran_i", DataType.DOUBLE)
												.addColumn("moran_zscore", DataType.DOUBLE)
												.build();
		Record record = DefaultRecord.of(outputSchema);
		record.set("moran_i", I);
		record.set("moran_zscore", zscore);
		
		
		StoreAsHeapfile.store(marmot, new Path(m_outputFilePath), RecordSet.of(record));
	}
	
	@Override
	public String toString() {
		return String.format("find_global_moran_I[layer=%s,target=%s]",
								m_dataset, m_targetColumn);
	}


	public static Builder builder() {
		return new Builder();
	}
	public static class Builder {
		private String m_layerName;
		private String m_targetColName;
		private double m_distance;
		private LISAWeight m_weightType = LISAWeight.FIXED_DISTANCE_BAND;
		private String m_outputDatasetName;
		
		public FindGlobalMoranI build() {
			return new FindGlobalMoranI(m_layerName, m_targetColName, m_distance,
											m_weightType, m_outputDatasetName);
		}
		
		/**
		 *  Getis-Ord Gi*를 구하고자 하는 레이어의 이름을 설정한다.
		 *  
		 * @param datasetName	레이어 이름.
		 * @return	설정된 빌더 객체
		 */
		public Builder fromDataSet(String datasetName) {
			m_layerName = datasetName;
			return this;
		}
		
		/**
		 * Getis-Ord Gi*를 구하고자 하는 대상 컬럼의 이름을 설정한다.
		 * 
		 * @param colName	컬럼 이름.
		 * @return	설정된 빌더 객체
		 */
		public Builder targetColumn(String colName) {
			m_targetColName = colName;
			return this;
		}
		
		/**
		 * 가중치 계산에 사용될 최대 거리값.
		 * 
		 * @param distance	최대 거리값 (단위: 미터)
		 * @return	설정된 빌더 객체
		 */
		public Builder distance(double distance) {
			Preconditions.checkArgument(distance >= 0, "distance must be larger or equal to zero");
			
			m_distance = distance;
			return this;
		}
		
		/**
		 * 결과 레코드가 저장될 출력 레이어의 이름을 설정한다.
		 * 
		 * @param name	출력 레이어 이름.
		 * @return	설정된 빌더 객체
		 */
		public Builder outputDataSet(String name) {
			m_outputDatasetName = name;
			return this;
		}
	}
}
