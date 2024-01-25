package marmot.module.geo.arc;

import java.util.List;
import java.util.Map;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.analysis.module.geo.arc.ArcUnionParameters;
import marmot.dataset.DataSet;
import marmot.module.MarmotModule;
import marmot.optor.StoreDataSetOptions;
import marmot.proto.optor.ArcUnionPhase1Proto;
import marmot.proto.optor.OperatorProto;
import marmot.type.DataType;
import utils.CSV;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ArcUnionProcess implements MarmotModule {
	private MarmotCore m_marmot;
	private ArcUnionParameters m_params;

	@Override
	public List<String> getParameterNameAll() {
		return ArcUnionParameters.getParameterNameAll();
	}
	
	@Override
	public RecordSchema getOutputRecordSchema(MarmotCore marmot, Map<String, String> paramsMap) {
		m_params = ArcUnionParameters.fromMap(paramsMap);
		m_params.checkValidity();
		
		DataSet input = marmot.getDataSet(m_params.getLeftDataSet());
		return input.getRecordSchema();
	}

	@Override
	public void initialize(MarmotCore marmot, Map<String, String> paramsMap) {
		m_marmot = marmot;
		m_params = ArcUnionParameters.fromMap(paramsMap);
		m_params.checkValidity();
	}

	@Override
	public void run() {
		DataSet left = validateGeometry(m_marmot, m_params.getLeftDataSet());
		DataSet right = validateGeometry(m_marmot, m_params.getRightDataSet());
		
		StoreDataSetOptions opts = StoreDataSetOptions.GEOMETRY(left.getGeometryColumnInfo());
		opts = m_params.getForce().transform(opts, StoreDataSetOptions::force);
		opts = m_params.getCompressionCodecName().transform(opts, StoreDataSetOptions::compressionCodecName);
		opts = m_params.getBlockSize().transform(opts, StoreDataSetOptions::blockSize);
		
		String geomCol = left.getGeometryColumn();
		String rightGeomCol = right.getGeometryColumn();
		
		//
		// 먼저 left와 right 데이터 세트중 겹치는 부분과 left에만 존재하는 영역을 추출한다.
		//
		performLeftOuterJoin(left, right);
		
		String leftCols = left.getRecordSchema().streamColumns()
								.map(Column::name)
								.filter(n -> !n.equals(geomCol))
								.join(',');
		String rightCols = right.getRecordSchema().streamColumns()
								.map(Column::name)
								.filter(n -> !n.equals(rightGeomCol))
								.join(',');
		
		String projectRightCols = String.format("%s,%s",
												rightGeomCol, ArcGisUtils.toProjectSuffixed(rightCols));
		String leftColDecls = CSV.parseCsv(leftCols)
								.map(key -> left.getRecordSchema().getColumn(key))
								.map(col -> String.format("%s:%s", col.name(), col.type().getName()))
								.join(',');
		String outCols = String.format("the_geom,%s,%s",
										leftCols, ArcGisUtils.toSuffixed(rightCols));
		
		Plan plan;
		plan = Plan.builder("arc_union_phase2")
						.load(m_params.getRightDataSet())
						.project(projectRightCols)
						.flattenGeometry(rightGeomCol, DataType.POLYGON)
						.differenceJoin(rightGeomCol, m_params.getLeftDataSet())
						.expand(leftColDecls)
						.project(outCols)
						.flattenGeometry(rightGeomCol, DataType.POLYGON)
						.store(m_params.getOutputDataset(), opts.force(false).append(true))
						.build();
		m_marmot.execute(plan);
	}
	
	@Override
	public String toString() {
		return String.format("ArcUnion(%s, %s) -> %s", m_params.getLeftDataSet(),
							m_params.getRightDataSet(), m_params.getOutputDataset());
	}
	
	private DataSet validateGeometry(MarmotCore marmot, String dsId) {
		DataSet ds = marmot.getDataSet(dsId);
		Column col = ds.getRecordSchema().getColumnAt(ds.getGeometryColumnIndex());
		DataType geomType = col.type();
		switch ( geomType.getTypeCode() ) {
			case POLYGON: case MULTI_POLYGON:
				break;
			default:
				throw new IllegalArgumentException("left dataset geometry is not POLYGON, type="
													+ geomType);
		}
		
		return ds;
	}
	
	private void performLeftOuterJoin(DataSet left, DataSet right) {
		StoreDataSetOptions opts = StoreDataSetOptions.GEOMETRY(left.getGeometryColumnInfo());
		opts = m_params.getForce().transform(opts, StoreDataSetOptions::force);
		opts = m_params.getCompressionCodecName().transform(opts, StoreDataSetOptions::compressionCodecName);
		opts = m_params.getBlockSize().transform(opts, StoreDataSetOptions::blockSize);
		
		String geomCol = left.getGeometryColumn();
		ArcUnionPhase1Proto phase1 = ArcUnionPhase1Proto.newBuilder()
														.setLeftGeomColumn(geomCol)
														.setRightDataset(right.getId())
														.build();
		OperatorProto op = OperatorProto.newBuilder()
										.setArcUnionPhase1(phase1)
										.build();
		Plan plan;
		plan = Plan.builder("arc_union_phase1")
						.load(m_params.getLeftDataSet())
						.flattenGeometry(geomCol, DataType.POLYGON)
						.add(op)
						.flattenGeometry(geomCol, DataType.POLYGON)
						.store(m_params.getOutputDataset(), opts)
						.build();
		m_marmot.execute(plan);
	}
}
