package marmot.module.geo.arc;

import static marmot.optor.AggregateFunction.CONCAT_STR;
import static marmot.optor.AggregateFunction.UNION_GEOM;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import marmot.MarmotCore;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.RecordSchema;
import marmot.analysis.module.geo.arc.ArcBufferParameters;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.module.MarmotModule;
import marmot.optor.StoreDataSetOptions;
import marmot.plan.Group;
import utils.UnitUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ArcBufferProcess implements MarmotModule {
	private MarmotCore m_marmot;
	private ArcBufferParameters m_params;

	@Override
	public List<String> getParameterNameAll() {
		return ArcBufferParameters.getParameterNameAll();
	}
	
	@Override
	public RecordSchema getOutputRecordSchema(MarmotCore marmot, Map<String, String> paramsMap) {
		m_params = ArcBufferParameters.fromMap(paramsMap);
		
		DataSet input = marmot.getDataSet(m_params.getInputDataset());
		return input.getRecordSchema();
	}

	@Override
	public void initialize(MarmotCore marmot, Map<String, String> paramsMap) {
		m_marmot = marmot;
		m_params = ArcBufferParameters.fromMap(paramsMap);
	}

	@Override
	public void run() {
		PlanBuilder builder = Plan.builder("arc_buffer")
									.load(m_params.getInputDataset());
		m_params.getDistanceColumn()
				.ifPresent(col -> builder.buffer("the_geom", col));
		m_params.getDistance()
				.ifPresent(dist -> builder.buffer("the_geom", dist));
		
		GeometryColumnInfo gcInfo = m_marmot.getDataSet(m_params.getInputDataset())
											.getGeometryColumnInfo();
		StoreDataSetOptions opts = StoreDataSetOptions.GEOMETRY(gcInfo);
		opts = m_params.getForce().transform(opts, StoreDataSetOptions::force);
		opts = m_params.getCompressionCodecName().transform(opts, StoreDataSetOptions::compressionCodecName);
		opts = m_params.getBlockSize().transform(opts, StoreDataSetOptions::blockSize);
		
		if ( !m_params.getDissolve() ) {
			builder.store(m_params.getOutputDataset(), opts);
			m_marmot.execute(builder.build());
			return;
		}
		
		builder.assignUid("FID");
		
		String tempOutDsId = "tmp/" + UUID.randomUUID().toString();
		builder.store(tempOutDsId, opts);
		m_marmot.execute(builder.build());
		DataSet output = m_marmot.getDataSet(tempOutDsId);
		
		boolean done = false;
		while ( !done ) {
			DataSet collapsed = collapse(m_marmot, output, opts);
			done = output.getRecordCount() == collapsed.getRecordCount();

			m_marmot.deleteDataSet(output.getId());
			output = collapsed;
		}
		
		if ( m_params.getForce().getOrElse(false) ) {
			m_marmot.deleteDataSet(m_params.getOutputDataset());
		}
		m_marmot.moveDataSet(output.getId(), m_params.getOutputDataset());
	}
	
	private DataSet collapse(MarmotCore marmot, DataSet ds, StoreDataSetOptions opts) {
		ds.createSpatialIndex();

		String tempId = "tmp/" + UUID.randomUUID().toString();
		Plan plan = Plan.builder("arc_buffer_dissolve_phase1")
						.loadSpatialIndexJoin(ds.getId(), ds.getId(),
											"left.*,right.{the_geom as the_geom2, FID as FID2}")
						.aggregateByGroup(Group.ofKeys("FID").orderBy("FID2:A"),
										UNION_GEOM("the_geom2").as("the_geom"),
										CONCAT_STR("FID2", ",").as("cover"))
						.takeByGroup(Group.ofKeys("cover"), 1)
						.project("the_geom")
						.assignUid("FID")
						.store(tempId, opts)
						.build();
		marmot.execute(plan);
		
		return marmot.getDataSet(tempId);
	}
	
	@Override
	public String toString() {
		String distStr;
		if ( m_params.getDistanceColumn().isPresent() ) {
			distStr = String.format("distance=%s", m_params.getDistanceColumn().get());
		}
		else {
			distStr = String.format("distance=%s",
									UnitUtils.toMeterString(m_params.getDistance().get()));
		}
		
		return String.format("ArcBuffer(%s,dissolve=%s,output=%s)",
							distStr, m_params.getDissolve(), m_params.getOutputDataset());
	}
}
