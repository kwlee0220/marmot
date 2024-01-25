package marmot.module;

import java.util.List;
import java.util.Map;

import marmot.MarmotCore;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.RecordScript;
import marmot.analysis.module.NormalizeParameters;
import marmot.analysis.module.PercentileRankParameters;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.StoreDataSetOptions;
import marmot.type.DataType;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PercentileRank implements MarmotModule {
	private MarmotCore m_marmot;
	private PercentileRankParameters m_params;

	@Override
	public List<String> getParameterNameAll() {
		return PercentileRankParameters.getParameterNameAll();
	}

	@Override
	public RecordSchema getOutputRecordSchema(MarmotCore marmot,
												Map<String, String> paramsMap) {
		NormalizeParameters params = NormalizeParameters.fromMap(paramsMap);
		DataSet input = marmot.getDataSet(params.inputDataset());
			
		RecordSchema.Builder builder = input.getRecordSchema().toBuilder();
		params.inputFeatureColumns()
				.stream()
				.forEach(name -> builder.addOrReplaceColumn(name, DataType.DOUBLE));
		return builder.build();
	}

	@Override
	public void initialize(MarmotCore marmot, Map<String, String> paramsMap) {
		m_marmot = marmot;
		m_params = PercentileRankParameters.fromMap(paramsMap);
	}

	@Override
	public void run() {
		String rankCol = m_params.rankColumn();
		String outCol = m_params.outputColumn();
		
		DataSet input = m_marmot.getDataSet(m_params.inputDataset());
		long recordCount = input.getRecordCount();

		Plan plan;
		String colDecl = String.format("%s:long", outCol);
		String initExpr = String.format("$ratio = 100.0 / %s", recordCount);
		String colInit = String.format("(long)Math.floor(100 - (%s * $ratio);", rankCol);
		
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
		plan = Plan.builder("attach_percentile_rank")
						.load(m_params.inputDataset())
						.expand(colDecl, RecordScript.of(initExpr, colInit))
						.store(m_params.outputDataset(), StoreDataSetOptions.FORCE(gcInfo))
						.build();
		m_marmot.execute(plan);
	}
	
	@Override
	public String toString() {
		String inColExpr = m_params.rankColumn();
		String outColExpr = m_params.outputColumn();
		
		return String.format("PercentileRank(%s{%s})->%s{%s}",
							m_params.inputDataset(), inColExpr,
							m_params.outputDataset(), outColExpr);
	}
}
