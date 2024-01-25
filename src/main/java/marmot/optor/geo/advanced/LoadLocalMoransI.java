package marmot.optor.geo.advanced;


import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.STDDEV;
import static marmot.optor.AggregateFunction.SUM;

import java.util.Map;
import java.util.UUID;

import javax.annotation.Nonnull;

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
import marmot.proto.optor.LoadLocalMoransIProto;
import marmot.support.PBSerializable;
import marmot.support.RecordSetOperatorChain;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LoadLocalMoransI extends CompositeRecordSetLoader
								implements PBSerializable<LoadLocalMoransIProto> {
	private final String m_dataset;
	private final String m_idColumn;
	private final String m_valueColumn;
	private final double m_radius;
	private final LISAWeight m_wtype;
	
	private LoadLocalMoransI(String layerName, String idColumn, String valueColumn,
							double radius, LISAWeight wtype) {
		Preconditions.checkArgument(layerName != null, "input layer name should not be null");
		Preconditions.checkArgument(idColumn != null, "id column name should not be null");
		Preconditions.checkArgument(valueColumn != null, "value column name should not be null");
		Preconditions.checkArgument(radius >= 0, "radius should not be null");
		Preconditions.checkArgument(wtype != null, "LISAWeight should not be null");
		
		m_dataset = layerName;
		m_idColumn = idColumn;
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
		
		return CalcLocalMoransI.getOutputRecordSchema(info.getRecordSchema());
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
		CalcLocalMoransI calc = new CalcLocalMoransI(info.getGeometryColumnInfo().name(),
													m_dataset, m_idColumn, m_valueColumn, m_radius,
													m_wtype, params);

		return RecordSetOperatorChain.from(m_marmot, Plan.builder()
														.load(m_dataset)
														.add(calc)
														.build());
	}
	
	@Override
	public String toString() {
		return String.format("LoadLocalMoran'sI[layer=%s,id=%s,target=%s]",
								m_dataset, m_idColumn, m_valueColumn);
	}
	
	public static LoadLocalMoransI fromProto(LoadLocalMoransIProto proto) {
		return LoadLocalMoransI.builder()
								.dataset(proto.getDataset())
								.idColumn(proto.getIdColumn())
								.valueColumn(proto.getValueColumn())
								.radius(proto.getRadius())
								.weightType(LISAWeight.valueOf(proto.getWeightType().name()))
								.build();
	}

	@Override
	public LoadLocalMoransIProto toProto() {
		return LoadLocalMoransIProto.newBuilder()
								.setDataset(m_dataset)
								.setIdColumn(m_idColumn)
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
		private String m_idColumn;
		private String m_valueColumn;
		private double m_radius;
		private LISAWeight m_wtype;
		
		public LoadLocalMoransI build() {
			return new LoadLocalMoransI(m_dataset, m_idColumn, m_valueColumn, m_radius,
										m_wtype);
		}
		
		/**
		 *  Local Moran's I를 구하고자 하는 레이어의 이름을 설정한다.
		 *  
		 * @param name	레이어 이름.
		 * @return	설정된 빌더 객체
		 */
		public Builder dataset(@Nonnull String name) {
			m_dataset = name;
			return this;
		}
		
		/**
		 *  Local Moran's I를 구할 때 레코드간 구별을 위해 사용되는 식별자 컬럼의 이름을 설정한다.
		 * 
		 * @param colName	컬럼 이름.
		 * @return	설정된 빌더 객체
		 */
		public Builder idColumn(@Nonnull String colName) {
			m_idColumn = colName;
			return this;
		}
		
		/**
		 *  Local Moran's I를 구하고자 하는 대상 컬럼의 이름을 설정한다.
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
		MarmotCore marmot = (MarmotCore)runtime;
		Map<String,Object> params = Maps.newHashMap();

		Path tempPath = new Path("tmp/" + UUID.randomUUID());
		Plan plan0 = Plan.builder("find_statistics")
						.load(m_dataset)
						.aggregate(COUNT(), AVG(m_valueColumn), STDDEV(m_valueColumn))
						.storeMarmotFile(tempPath.toString())
						.build();
		MultiJobPlanExecution exec = MultiJobPlanExecution.create(marmot, plan0);
		exec.start();
		
		Record result = LoadMarmotFile.readARecord(marmot, tempPath);
		params.putAll(result.toMap());
		marmot.getFileServer().deleteFile(tempPath);

		double avg = (Double)params.get("avg");
		String schemaStr = "diff:double,diff2:double,diff4:double";
		String updateExpr = String.format("diff=%s-%f; diff2=diff*diff; diff4=diff2*diff2",
											m_valueColumn, avg);

		Plan plan1 = Plan.builder("find_statistics2")
						.load(m_dataset)
						.expand(schemaStr, updateExpr)
						.aggregate(SUM("diff").as("diffSum"),
									SUM("diff2").as("diff2Sum"),
									SUM("diff4").as("diff4Sum"))
						.storeMarmotFile(tempPath.toString())
						.build();
		MultiJobPlanExecution exec1 = MultiJobPlanExecution.create(marmot, plan1);
		exec1.start();

		result = LoadMarmotFile.readARecord(marmot, tempPath);
		params.putAll(result.toMap());
		marmot.getFileServer().deleteFile(tempPath);
		
		return params;
	}
}
