package marmot.module.geo.arc;

import java.util.List;
import java.util.Map;

import com.google.common.base.Objects;

import marmot.MarmotCore;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.analysis.module.geo.arc.ArcMergeParameters;
import marmot.dataset.DataSet;
import marmot.dataset.DataSetImpl;
import marmot.dataset.GeometryColumnInfo;
import marmot.module.MarmotModule;
import marmot.optor.StoreDataSetOptions;
import marmot.plan.LoadOptions;
import utils.CSV;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ArcMergeProcess implements MarmotModule {
	private MarmotCore m_marmot;
	private ArcMergeParameters m_params;
	private List<DataSetImpl> m_inputDatasets;

	@Override
	public List<String> getParameterNameAll() {
		return ArcMergeParameters.getParameterNameAll();
	}

	@Override
	public RecordSchema getOutputRecordSchema(MarmotCore marmot, Map<String, String> params) {
		m_params = ArcMergeParameters.fromMap(params);
		
		return checkInputDatasets(marmot, m_params).get(0).getRecordSchema();
	}

	@Override
	public void initialize(MarmotCore marmot, Map<String, String> paramsMap) {
		m_marmot = marmot;
		m_params = ArcMergeParameters.fromMap(paramsMap);
		m_inputDatasets = checkInputDatasets(marmot, m_params);
	}

	@Override
	public void run() {
		StoreDataSetOptions opts = StoreDataSetOptions.DEFAULT;
		opts = m_params.getForce().transform(opts, StoreDataSetOptions::force);
		opts = m_params.getCompressionCodecName().transform(opts, StoreDataSetOptions::compressionCodecName);
		opts = m_params.getBlockSize().transform(opts, StoreDataSetOptions::blockSize);

		m_inputDatasets = checkInputDatasets(m_marmot, m_params);
		DataSet first = m_inputDatasets.get(0);
		if (first.hasGeometryColumn() ) {
			GeometryColumnInfo gcInfo = first.getGeometryColumnInfo();
			opts = opts.geometryColumnInfo(gcInfo);
		}
		
		List<String> dsIdList = FStream.from(m_inputDatasets).map(DataSet::getId).toList();
		Plan plan = Plan.builder("ArcMerge")
						.load(dsIdList, LoadOptions.DEFAULT)
						.store(m_params.getOutputDataset(), opts)
						.build();
		m_marmot.execute(plan);
	}
	
	@Override
	public String toString() {
		return String.format("ArcMerge(%s)->%s", m_params.getInputDatasets(),
								m_params.getOutputDataset());
	}
	
	private List<DataSetImpl> checkInputDatasets(MarmotCore marmot, ArcMergeParameters params) {
		List<DataSetImpl> dsList = CSV.parseCsv(m_params.getInputDatasets(), ',')
										.map(id -> marmot.getDataSet(id))
										.toList();
		if ( dsList.isEmpty() ) {
			throw new IllegalArgumentException("empty input dataset list is empty");
		}
		
		DataSetImpl first = dsList.get(0);
		RecordSchema schema = first.getRecordSchema();
		GeometryColumnInfo gcInfo = first.hasGeometryColumn() ? first.getGeometryColumnInfo() : null;
		
		for ( DataSetImpl ds: dsList ) {
			if ( !schema.equals(ds.getRecordSchema()) ) {
				String msg = String.format("Incompatible RecordSet: %s <-> %s", first.getId(), ds.getId());
				throw new IllegalArgumentException(msg);
			}
			
			GeometryColumnInfo info = ds.hasGeometryColumn() ? ds.getGeometryColumnInfo() : null;
			if ( !Objects.equal(gcInfo, info) ) {
				String msg = String.format("Incompatible GeometryColumnInfo: %s <-> %s", gcInfo, info);
				throw new IllegalArgumentException(msg);
			}
		}
		
		return dsList;
	}
}
