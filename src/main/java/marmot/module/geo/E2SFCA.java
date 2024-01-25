package marmot.module.geo;

import static marmot.optor.StoreDataSetOptions.FORCE;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import marmot.MarmotCore;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.RecordSchema;
import marmot.analysis.module.geo.E2SFCAParameters;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.module.MarmotModule;
import marmot.optor.geo.advanced.InterpolationMethod;
import marmot.optor.geo.advanced.WeightedSum;
import marmot.type.DataType;
import utils.CSV;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class E2SFCA implements MarmotModule {
	private MarmotCore m_marmot;
	private E2SFCAParameters m_params;
	private String m_ratioCols;
	private InterpolationMethod m_method;

	@Override
	public List<String> getParameterNameAll() {
		return E2SFCAParameters.getParameterNameAll();
	}

	@Override
	public RecordSchema getOutputRecordSchema(MarmotCore marmot, Map<String, String> paramsMap) {
		E2SFCAParameters params = E2SFCAParameters.fromMap(paramsMap);
		DataSet input = marmot.getDataSet(params.getConsumerDataset());
		
		return CSV.parseCsv(params.getOutputFeatureColumns(), ',', '\\')
					.collect(input.getRecordSchema().toBuilder(),
								(b,n) -> b.addOrReplaceColumn(n, DataType.DOUBLE))
					.build();
	}

	@Override
	public void initialize(MarmotCore marmot, Map<String, String> paramsMap) {
		m_marmot = marmot;
		m_params = E2SFCAParameters.fromMap(paramsMap);
	}

	@Override
	public void run() {
		m_method = WeightedSum.with(m_params.getWeightFunction());
		List<String> inFeatureColList = CSV.parseCsv(m_params.getConsumerFeatureColumns()).toList();
		m_ratioCols = FStream.range(0, inFeatureColList.size())
							.mapToObj(idx -> String.format("_r_%02d", idx))
							.join(",");
		String tempOutput = "tmp/" + UUID.randomUUID().toString();
		try {
			performStep1(m_marmot, tempOutput);
			performStep2(m_marmot, tempOutput);
		}
		finally {
			m_marmot.deleteDataSet(tempOutput);
		}
	}
	
	@Override
	public String toString() {
		return String.format("E2SFCA(%s,%s)->%s", m_params.getConsumerDataset(),
								m_params.getProviderDataset(), m_params.getOutputDataset());
	}
	
	private void performStep1(MarmotCore marmot, String outDsId) {
		DataSet ds = marmot.getDataSet(m_params.getProviderDataset());
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		
		String updateExpr = CSV.parseCsv(m_params.getProviderFeatureColumns(), ',', '\\')
								.zipWith(CSV.parseCsv(m_ratioCols, ',', '\\'))
								.map(t -> String.format("%s = %s / %s;", t._2, t._1, t._2))
								.join(" ");
		
		PlanBuilder builder;
		builder = Plan.builder("E2SFCA_Step1")
					.load(m_params.getProviderDataset());
		builder = builder.interpolateSpatially(gcInfo.name(), m_params.getConsumerDataset(),
												m_params.getConsumerFeatureColumns(),
												m_params.getServiceDistance(), m_ratioCols,
												m_method);
		Plan plan = builder.update(updateExpr)
							.project(gcInfo.name() + "," + m_ratioCols)
							.store(outDsId, FORCE(gcInfo))
							.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(outDsId);
		result.createSpatialIndex();
	}
	
	private void performStep2(MarmotCore marmot, String intermDataSet) {
		DataSet ds = marmot.getDataSet(m_params.getConsumerDataset());
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		
		PlanBuilder builder;
		Plan plan = Plan.builder("E2SFCA_Step2")
						.load(m_params.getConsumerDataset())
						.interpolateSpatially(gcInfo.name(), intermDataSet, m_ratioCols,
												m_params.getServiceDistance(), 
												m_params.getOutputFeatureColumns(),
												m_method)
						.store(m_params.getOutputDataset(), FORCE(gcInfo))
						.build();
		marmot.execute(plan);
	}
}
