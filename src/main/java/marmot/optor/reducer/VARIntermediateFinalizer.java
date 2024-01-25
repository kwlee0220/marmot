package marmot.optor.reducer;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.optor.support.RecordLevelTransform;
import marmot.proto.optor.VARIntermediateFinalizerProto;
import marmot.support.PBSerializable;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class VARIntermediateFinalizer extends RecordLevelTransform
										implements PBSerializable<VARIntermediateFinalizerProto> {
	private final ValueAggregate[] m_aggrs;
	
	public VARIntermediateFinalizer(ValueAggregate[] aggrs) {
		m_aggrs = aggrs;
	}
	
	@Override
	public void initialize(MarmotCore marmot, RecordSchema intermediateSchema) {
		RecordSchema.Builder builder = RecordSchema.builder();
		for ( int i =0; i < m_aggrs.length; ++i ) {
			ValueAggregate aggr = m_aggrs[i];
			
			aggr.initializeWithIntermediate(intermediateSchema);
			aggr.getOutputValueSchema()
				.streamColumns()
				.forEach(builder::addColumn);
		}
		RecordSchema outSchema = builder.build();
		
		setInitialized(marmot, intermediateSchema, outSchema);
	}

	@Override
	public boolean transform(Record intermediate, Record output) {
		for ( ValueAggregate aggr: m_aggrs ) {
			aggr.toFinal(intermediate, output);
		}
		
		return true;
	}
	
	@Override
	public String toString() {
		String aggrsStr = FStream.of(m_aggrs)
								.map(ValueAggregate::toString)
								.join(",");
		return String.format("finalize_intermediate[%s]", aggrsStr);
	}

	public static VARIntermediateFinalizer fromProto(VARIntermediateFinalizerProto proto) {
		ValueAggregate[] aggrs = proto.getAggregatorsList()
										.stream()
										.map(ValueAggregates::fromProto)
										.toArray(sz -> new ValueAggregate[sz]);
		return new VARIntermediateFinalizer(aggrs);
	}
	
	@Override
	public VARIntermediateFinalizerProto toProto() {
		return FStream.of(m_aggrs)
						.map(ValueAggregates::toProto)
						.fold(VARIntermediateFinalizerProto.newBuilder(),
								(b,aggr) -> b.addAggregators(aggr))
						.build();
	}
}