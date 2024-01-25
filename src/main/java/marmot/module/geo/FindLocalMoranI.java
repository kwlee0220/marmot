package marmot.module.geo;

import static marmot.optor.StoreDataSetOptions.FORCE;

import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import marmot.MarmotCore;
import marmot.Plan;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.catalog.DataSetInfo;
import marmot.optor.geo.advanced.LISAWeight;
import marmot.optor.geo.advanced.LoadLocalMoransI;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindLocalMoranI implements Consumer<MarmotCore> {
	private final String m_layerName;
	private final String m_idColumn;
	private final String m_valueColumn;
	private final double m_radius;
	private final LISAWeight m_wtype;
	private final String m_outputLayerName;
	
	private FindLocalMoranI(String layerName, String idColumn, String valueColumn,
							double radius, LISAWeight wtype, String outputLayerName) {
		Preconditions.checkArgument(layerName != null, "input layer name should not be null");
		Preconditions.checkArgument(valueColumn != null, "value column name should not be null");
		Preconditions.checkArgument(radius >= 0, "radius must be larger or equal to zero");
		Preconditions.checkArgument(wtype != null, "LISAWeight should not be null");
		Preconditions.checkArgument(outputLayerName != null, "output layer name should not be null");
		
		m_layerName = layerName;
		m_idColumn = idColumn;
		m_valueColumn = valueColumn;
		m_radius = radius;
		m_wtype = wtype;
		m_outputLayerName = outputLayerName;
	}

	@Override
	public void accept(MarmotCore marmot) {
		DataSetInfo info = marmot.getDataSetInfo(m_layerName);
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom",
														info.getGeometryColumnInfo().get().srid());
		LoadLocalMoransI load = LoadLocalMoransI.builder()
												.dataset(m_layerName)
												.idColumn(m_idColumn)
												.valueColumn(m_valueColumn)
												.radius(m_radius)
												.weightType(m_wtype)
												.build();
		Plan plan = Plan.builder("load_" + load)
						.add(load)
						.store(m_outputLayerName, FORCE(gcInfo))
						.build();
		marmot.execute(plan);
	}
	
	@Override
	public String toString() {
		return String.format("LocalMoranI[layer=%s,target=%s,output=%s]",
							m_layerName, m_valueColumn, m_outputLayerName);
	}
	
	public static Builder builder() {
		return new Builder();
	}
	public static class Builder {
		private String m_layerName;
		private String m_idColumn;
		private String m_valueColumn;
		private double m_distance;
		private LISAWeight m_wtype = LISAWeight.FIXED_DISTANCE_BAND;
		private String m_outputLayerName;
		
		public FindLocalMoranI build() {
			return new FindLocalMoranI(m_layerName, m_idColumn, m_valueColumn,
										m_distance, m_wtype, m_outputLayerName);
		}
		
		/**
		 *  Local Moran's I를 구하고자 하는 레이어의 이름을 설정한다.
		 *  
		 * @param layerName	레이어 이름.
		 * @return	설정된 빌더 객체
		 */
		public Builder fromLayer(@Nonnull String layerName) {
			m_layerName = layerName;
			return this;
		}
		
		public Builder idColumn(@Nonnull String colName) {
			m_idColumn = colName;
			return this;
		}
		
		/**
		 * Local Moran's I를 구하고자 하는 대상 컬럼의 이름을 설정한다.
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
		 * LISA weight type을 설정한다.
		 * 
		 * @param wtype	 LISA weight type
		 * @return	설정된 빌더 객체
		 */
		public Builder weightType(LISAWeight wtype) {
			Preconditions.checkArgument(wtype != null, "LISAWeight is null");
			
			m_wtype = wtype;
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
