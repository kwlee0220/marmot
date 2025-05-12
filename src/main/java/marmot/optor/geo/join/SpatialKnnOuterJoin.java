package marmot.optor.geo.join;

import java.util.Map;

import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSets;
import marmot.optor.RecordSetOperatorException;
import marmot.optor.support.JoinUtils;
import marmot.optor.support.colexpr.ColumnSelector;
import marmot.plan.SpatialJoinOptions;
import marmot.proto.optor.SpatialKnnOuterJoinProto;
import marmot.support.PBSerializable;

import utils.Tuple;
import utils.func.FOption;
import utils.stream.PrependableFStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialKnnOuterJoin extends SpatialKnnJoin<SpatialKnnOuterJoin>
							implements PBSerializable<SpatialKnnOuterJoinProto> {
	private ColumnSelector m_selector;
	
	public SpatialKnnOuterJoin(String inputGeomCol, String paramDsId, int topK, double dist,
								SpatialJoinOptions opts) {
		super(inputGeomCol, paramDsId, dist, FOption.of(topK), opts);

		setLogger(LoggerFactory.getLogger(SpatialKnnOuterJoin.class));
	}

	@Override
	protected RecordSchema initialize(MarmotCore marmot, RecordSchema outerSchema,
										RecordSchema innerSchema) {
		try {
			m_selector = JoinUtils.createJoinColumnSelector(outerSchema, innerSchema,
															m_options.outputColumns());
			return m_selector.getRecordSchema();
		}
		catch ( Exception e ) {
 			throw new RecordSetOperatorException(String.format("op=%s, cause=%s", this, e));
		}
	}

	@Override
	protected RecordSet handleKnnMatches(NestedLoopKnnMatch match) {
		PrependableFStream<Tuple<Record,Double>> peekable = match.getInnerRecords().toPrependable();
		if ( peekable.hasNext() ) {
			RecordSet inners = RecordSet.from(match.getInnerRecordSchema(),
												peekable.map(Tuple::_1));
			return m_selector.select(match.getOuterRecord(), inners);
		}
		else {
			return RecordSets.singleton(getRecordSchema(), record -> {
				Map<String,Record> binding = Maps.newHashMap();
				binding.put("", match.getOuterRecord());
				m_selector.select(binding, record);
			});
		}
	}
	
	@Override
	public String toString() {
		return String.format("spatial_knn_outer_join[k=%d, r=%.1f%s]",
								getTopK(), getRadius());
	}

	public static SpatialKnnOuterJoin fromProto(SpatialKnnOuterJoinProto proto) {
		SpatialJoinOptions opts = SpatialJoinOptions.fromProto(proto.getOptions());
		return new SpatialKnnOuterJoin(proto.getGeomColumn(), proto.getParamDataset(),
										proto.getTopK(), proto.getRadius(), opts);
	}

	@Override
	public SpatialKnnOuterJoinProto toProto() {
		return SpatialKnnOuterJoinProto.newBuilder()
										.setGeomColumn(getOuterGeomColumnName())
										.setParamDataset(getParamDataSetId())
										.setTopK(getTopK().get())
										.setRadius(getRadius())
										.setOptions(m_options.toProto())
										.build();
	}
}
