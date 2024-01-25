package marmot.optor.geo.join;

import org.slf4j.LoggerFactory;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.RecordSetOperatorException;
import marmot.optor.support.JoinUtils;
import marmot.optor.support.colexpr.ColumnSelector;
import marmot.plan.SpatialJoinOptions;
import marmot.proto.optor.SpatialKnnInnerJoinProto;
import marmot.support.PBSerializable;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialKnnInnerJoin extends SpatialKnnJoin<SpatialKnnInnerJoin>
								implements PBSerializable<SpatialKnnInnerJoinProto> {
	private ColumnSelector m_selector;
	
	public SpatialKnnInnerJoin(String inputGeomCol, String paramDsId, FOption<Integer> topK,
								double dist, SpatialJoinOptions opts) {
		super(inputGeomCol, paramDsId, dist, topK, opts);
		
		setLogger(LoggerFactory.getLogger(SpatialKnnOuterJoin.class));
	}
	public SpatialKnnInnerJoin(String inputGeomCol, String paramDsId, double dist,
								SpatialJoinOptions opts) {
		super(inputGeomCol, paramDsId, dist, FOption.empty(), opts);
		
		setLogger(LoggerFactory.getLogger(SpatialKnnOuterJoin.class));
	}

	@Override
	protected RecordSchema initialize(MarmotCore marmot, RecordSchema leftSchema,
										RecordSchema rightSchema) {
		try {
			m_selector = JoinUtils.createJoinColumnSelector(leftSchema, rightSchema,
															m_options.outputColumns());
			return m_selector.getRecordSchema();
		}
		catch ( Exception e ) {
 			throw new RecordSetOperatorException(String.format("op=%s, cause=%s", this, e));
		}
	}

	@Override
	protected RecordSet handleKnnMatches(NestedLoopKnnMatch match) {
		return m_selector.select(match.getOuterRecord(), match.getInnerRecordSet());
	}

	public static SpatialKnnInnerJoin fromProto(SpatialKnnInnerJoinProto proto) {
		SpatialJoinOptions opts = SpatialJoinOptions.fromProto(proto.getOptions());
		FOption<Integer> topK;
		switch ( proto.getOptionalTopKCase() ) {
			case TOP_K:
				topK = FOption.of(proto.getTopK());
				break;
			case OPTIONALTOPK_NOT_SET:
				topK = FOption.empty();
				break;
			default:
				throw new AssertionError();
		}
		
		return new SpatialKnnInnerJoin(proto.getGeomColumn(), proto.getParamDataset(), topK,
										proto.getRadius(), opts);
	}

	@Override
	public SpatialKnnInnerJoinProto toProto() {
		SpatialKnnInnerJoinProto.Builder builder = SpatialKnnInnerJoinProto.newBuilder()
										.setGeomColumn(getOuterGeomColumnName())
										.setParamDataset(getParamDataSetId())
										.setRadius(getRadius())
										.setOptions(m_options.toProto());
		builder = getTopK().transform(builder, (b,k) -> b.setTopK(k));
		return builder.build();
	}
}
