package marmot.module.geo;

import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.STDDEV;
import static marmot.optor.StoreDataSetOptions.FORCE;

import java.util.UUID;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;

import marmot.MarmotCore;
import marmot.Plan;
import marmot.Record;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.catalog.DataSetInfo;
import marmot.optor.LoadMarmotFile;
import marmot.optor.geo.advanced.LoadGetisOrdGi;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindGetisOrdGiStar implements Consumer<MarmotCore> {
	private final String m_dataset;
	private final String m_valueColumn;
	private final double m_radius;
	private final String m_outputDataset;
	
	private FindGetisOrdGiStar(String dataset, String valueColumn, double radius,
								String outputDataSet) {
		Preconditions.checkArgument(dataset != null, "input dataset name should not be null");
		Preconditions.checkArgument(valueColumn != null, "value column name should not be null");
		Preconditions.checkArgument(radius >= 0, "radius must be larger or equal to zero");
		Preconditions.checkArgument(outputDataSet != null, "output layer name should not be null");
		
		m_dataset = dataset;
		m_valueColumn = valueColumn;
		m_radius = radius;
		m_outputDataset = outputDataSet;
	}

	@Override
	public void accept(MarmotCore marmot) {
//		Map<ColumnName,Object> params = Maps.newHashMap();
		
		Path tempPath = new Path("tmp/" + UUID.randomUUID());
		Plan plan0 = Plan.builder("find_statistics")
						.load(m_dataset)
						.aggregate(COUNT(), AVG(m_valueColumn), STDDEV(m_valueColumn))
						.storeMarmotFile(tempPath.toString())
						.build();
		marmot.execute(plan0);
		
		
		Record result = LoadMarmotFile.readARecord(marmot, tempPath);
//		params.putAll(result.toMap());
		marmot.getFileServer().deleteFile(tempPath);
		
		LoadGetisOrdGi load = LoadGetisOrdGi.builder()
											.dataset(m_dataset)
											.valueColumn(m_valueColumn)
											.radius(m_radius)
											.build();
		
		DataSetInfo info = marmot.getDataSetInfo(m_dataset);
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom",
															info.getGeometryColumnInfo().get().srid());
		plan0 = Plan.builder("find_Getis-Ord-Gi*")
					.add(load)
					.store(m_outputDataset, FORCE(gcInfo))
					.build();
		marmot.execute(plan0);
	}
	
	@Override
	public String toString() {
		return String.format("LocalMoranI[layer=%s,target=%s,output=%s]",
							m_dataset, m_valueColumn, m_outputDataset);
	}
	
	public static Builder builder() {
		return new Builder();
	}
	public static class Builder {
		private String m_layerName;
		private String m_valueColumn;
		private double m_distance;
		private String m_outputLayerName;
		
		public FindGetisOrdGiStar build() {
			return new FindGetisOrdGiStar(m_layerName, m_valueColumn, m_distance,
										m_outputLayerName);
		}
		
		/**
		 *  Getis-Ord Gi*를 구하고자 하는 레이어의 이름을 설정한다.
		 *  
		 * @param layerName	레이어 이름.
		 * @return	설정된 빌더 객체
		 */
		public Builder fromLayer(@Nonnull String layerName) {
			m_layerName = layerName;
			return this;
		}
		
		/**
		 * Getis-Ord Gi*를 구하고자 하는 대상 컬럼의 이름을 설정한다.
		 * 
		 * @param colName	컬럼 이름.
		 * @return	설정된 빌더 객체
		 */
		public Builder valueColumn(@Nonnull String colName) {
			m_valueColumn = colName;
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
		 * @param layerName	출력 레이어 이름.
		 * @return	설정된 빌더 객체
		 */
		public Builder outputLayer(@Nonnull String layerName) {
			m_outputLayerName = layerName;
			return this;
		}
	}
}
