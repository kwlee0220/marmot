package marmot.module.geo.arc;

import java.util.List;
import java.util.Map;

import marmot.MarmotCore;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.analysis.module.geo.arc.ArcSplitParameters;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.module.MarmotModule;
import marmot.optor.StoreDataSetOptions;
import marmot.plan.Group;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ArcSplitProcess implements MarmotModule {
	private MarmotCore m_marmot;
	private ArcSplitParameters m_params;

	@Override
	public List<String> getParameterNameAll() {
		return ArcSplitParameters.getParameterNameAll();
	}

	@Override
	public RecordSchema getOutputRecordSchema(MarmotCore marmot, Map<String, String> paramsMap) {
		ArcSplitParameters params = ArcSplitParameters.fromMap(paramsMap);
		DataSet input = marmot.getDataSet(params.getInputDataset());
		DataSet split = marmot.getDataSet(params.getOutputDataset());
		
		if ( !split.getRecordSchema().existsColumn(params.getSplitKey()) ) {
			String msg = String.format("split key column does not exist: %s", params.getSplitKey());
			throw new IllegalArgumentException(msg);
		}
		
		return input.getRecordSchema();
	}

	@Override
	public void initialize(MarmotCore marmot, Map<String, String> paramsMap) {
		m_marmot = marmot;
		m_params = ArcSplitParameters.fromMap(paramsMap);
	}

	@Override
	public void run() {
		String splitKey = m_params.getSplitKey();
		
		StoreDataSetOptions opts = StoreDataSetOptions.DEFAULT;
		opts = m_params.getForce().transform(opts, StoreDataSetOptions::force);
		opts = m_params.getCompressionCodecName().transform(opts, StoreDataSetOptions::compressionCodecName);
		opts = m_params.getBlockSize().transform(opts, StoreDataSetOptions::blockSize);

		DataSet input = m_marmot.getDataSet(m_params.getInputDataset());
		if ( input.hasGeometryColumn() ) {
			GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
			opts = opts.geometryColumnInfo(gcInfo);
		}
		
		String joinOutCols = String.format("left.*,right.{%s}", splitKey);
		Plan plan = Plan.builder("ArcSplit")
						.loadSpatialIndexJoin(m_params.getInputDataset(),
												m_params.getSplitDataset(), joinOutCols)
						.storeByGroup(Group.ofKeys(splitKey), m_params.getOutputDataset(), opts)
						.build();
		m_marmot.execute(plan);
	}
	
	@Override
	public String toString() {
		return String.format("ArcSplit(%s,%s:%s)->%s", m_params.getInputDataset(),
								m_params.getSplitDataset(), m_params.getSplitKey(),
								m_params.getOutputDataset());
	}
}
