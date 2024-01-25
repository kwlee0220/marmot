package marmot.optor.reducer;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.optor.support.RecordLevelTransform;
import marmot.proto.optor.VARIntermediateProducerProto;
import marmot.support.PBSerializable;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class VARIntermediateProducer extends RecordLevelTransform
									implements PBSerializable<VARIntermediateProducerProto> {
	private final ValueAggregate[] m_aggrs;
	
	public VARIntermediateProducer(ValueAggregate[] aggrs) {
		m_aggrs = aggrs;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		RecordSchema.Builder builder = RecordSchema.builder();
		for ( int i =0; i < m_aggrs.length; ++i ) {
			ValueAggregate aggr = m_aggrs[i];
			
			aggr.initializeWithInput(inputSchema);
			aggr.getIntermediateValueSchema()
				.streamColumns()
				.forEach(builder::addColumn);
		}
		RecordSchema intermSchema = builder.build();
		FStream.of(m_aggrs).forEach(aggr -> aggr.setIntermediateSchema(intermSchema));
		
		setInitialized(marmot, inputSchema, intermSchema);
	}

	@Override
	public boolean transform(Record input, Record output) {
		FStream.of(m_aggrs)
				.forEach(aggr -> aggr.toIntermediate(input, output));
		
		return true;
	}
	
	@Override
	public String toString() {
		String aggrsStr = FStream.of(m_aggrs)
								.map(ValueAggregate::toString)
								.join(",");
		return String.format("%s: %s", getClass().getSimpleName(), aggrsStr);
	}

	public static VARIntermediateProducer fromProto(VARIntermediateProducerProto proto) {
		ValueAggregate[] aggrs = proto.getAggregatorsList()
										.stream()
										.map(ValueAggregates::fromProto)
										.toArray(sz -> new ValueAggregate[sz]);
		return new VARIntermediateProducer(aggrs);
	}
	
	@Override
	public VARIntermediateProducerProto toProto() {
		return FStream.of(m_aggrs)
						.map(ValueAggregates::toProto)
						.fold(VARIntermediateProducerProto.newBuilder(),
								(b,aggr) -> b.addAggregators(aggr))
						.build();
	}
}