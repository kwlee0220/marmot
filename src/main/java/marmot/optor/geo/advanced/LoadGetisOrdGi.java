package marmot.optor.geo.advanced;

import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.STDDEV;

import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import marmot.MarmotCore;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSetException;
import marmot.dataset.DataSet;
import marmot.mapreduce.MultiJobPlanExecution;
import marmot.optor.CompositeRecordSetLoader;
import marmot.optor.LoadMarmotFile;
import marmot.proto.optor.LISAWeightProto;
import marmot.proto.optor.LoadGetisOrdGiProto;
import marmot.support.PBSerializable;
import marmot.support.RecordSetOperatorChain;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LoadGetisOrdGi extends CompositeRecordSetLoader
							implements PBSerializable<LoadGetisOrdGiProto> {
	private final String m_dataset;
	private final String m_valueColumn;
	private final double m_radius;
	private final LISAWeight m_wtype;
	
	private LoadGetisOrdGi(String dataset, String valueColumn, double radius,
							LISAWeight wtype) {
		Preconditions.checkArgument(dataset != null, "input dataset name should not be null");
		Preconditions.checkArgument(valueColumn != null, "value column name should not be null");
		Preconditions.checkArgument(radius >= 0, "radius should not be null");
		Preconditions.checkArgument(wtype != null, "LISAWeight should not be null");
		
		m_dataset = dataset;
		m_valueColumn = valueColumn;
		m_radius = radius;
		m_wtype = wtype;
	}

	@Override
	protected RecordSchema _initialize(MarmotCore marmot) {
		DataSet info = marmot.getDataSet(m_dataset);
		if ( !info.hasGeometryColumn() ) {
			throw new RecordSetException("dataset does not have the default "
										+ "Geometry column: dataset=" + m_dataset);
		}
		
		return CalcGetisOrdGi.getOutputRecordSchema(info.getRecordSchema());
	}
	
	@Override
	protected RecordSetOperatorChain createComponents() {
		DataSet info = m_marmot.getDataSet(m_dataset);
		if ( !info.hasGeometryColumn() ) {
			throw new RecordSetException("dataset does not have the default "
										+ "Geometry column: dataset=" + m_dataset);
		}
		
		if ( !info.getRecordSchema().existsColumn(m_valueColumn) ) {
			throw new IllegalArgumentException("unknown value column: name=" + m_valueColumn
												+ ", dataset=" + m_dataset);
			
		}
		
		Map<String,Object> params = loadParameters(m_marmot);
		CalcGetisOrdGi calc = new CalcGetisOrdGi(info.getGeometryColumnInfo().name(),
												m_dataset, m_valueColumn, m_radius, params);

		return RecordSetOperatorChain.from(m_marmot, Plan.builder()
														.load(m_dataset)
														.add(calc)
														.build());
	}
	
	@Override
	public String toString() {
		return String.format("LoadGetisOrdGi*[layer=%s,value=%s]", m_dataset, m_valueColumn);
	}
	
	public static LoadGetisOrdGi fromProto(LoadGetisOrdGiProto proto) {
		return LoadGetisOrdGi.builder()
							.dataset(proto.getDataset())
							.valueColumn(proto.getValueColumn())
							.radius(proto.getRadius())
							.weightType(LISAWeight.valueOf(proto.getWeightType().name()))
							.build();
	}

	@Override
	public LoadGetisOrdGiProto toProto() {
		return LoadGetisOrdGiProto.newBuilder()
								.setDataset(m_dataset)
								.setValueColumn(m_valueColumn)
								.setRadius(m_radius)
								.setWeightType(LISAWeightProto.valueOf(m_wtype.name()))
								.build();
	}
	
	public static Builder builder() {
		return new Builder();
	}
	public static class Builder {
		private String m_dataset;
		private String m_valueColumn;
		private double m_radius;
		private LISAWeight m_wtype = LISAWeight.FIXED_DISTANCE_BAND;
		
		public LoadGetisOrdGi build() {
			return new LoadGetisOrdGi(m_dataset, m_valueColumn, m_radius, m_wtype);
		}
		
		/**
		 *  Getis-Ord Gi*를 구하고자 하는 레이어의 이름을 설정한다.
		 *  
		 * @param name	레이어 이름.
		 * @return	설정된 빌더 객체
		 */
		public Builder dataset(String name) {
			m_dataset = name;
			return this;
		}
		
		/**
		 * Getis-Ord Gi*를 구하고자 하는 대상 컬럼의 이름을 설정한다.
		 * 
		 * @param colName	컬럼 이름.
		 * @return	설정된 빌더 객체
		 */
		public Builder valueColumn(String colName) {
			m_valueColumn = colName;
			return this;
		}
		
		/**
		 * 가중치 계산에 사용될 최대 거리값.
		 * 
		 * @param radius	최대 거리값 (단위: 미터)
		 * @return	설정된 빌더 객체
		 */
		public Builder radius(double radius) {
			Preconditions.checkArgument(radius >= 0, "radius must be larger or equal to zero");
			
			m_radius = radius;
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
	}

	private Map<String,Object> loadParameters(MarmotCore runtime) {
		Map<String,Object> params = Maps.newHashMap();
		
		Path tempPath = new Path("tmp/" + UUID.randomUUID());
		Plan plan0 = Plan.builder("find_statistics")
								.load(m_dataset)
								.aggregate(COUNT(), AVG(m_valueColumn), STDDEV(m_valueColumn))
								.storeMarmotFile(tempPath.toString())
								.build();
		MarmotCore marmot = (MarmotCore)runtime;
		MultiJobPlanExecution exec = MultiJobPlanExecution.create(marmot, plan0);
		exec.start();
		
		Record result = LoadMarmotFile.readARecord(getMarmotCore(), tempPath);
		params.putAll(result.toMap());
		marmot.getFileServer().deleteFile(tempPath);
		
		return params;
	}
}
