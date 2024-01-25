package marmot.optor.reducer;

import utils.stream.FStream;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.proto.optor.VARIntermediateCombinerProto;
import marmot.support.PBSerializable;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class VARIntermediateReducer extends AbstractRecordSetReducer
								implements PBSerializable<VARIntermediateCombinerProto> {
	private final ValueAggregate[] m_aggrs;
	
	public VARIntermediateReducer(ValueAggregate[] aggrs) {
		m_aggrs = aggrs;
	}
	
	@Override
	public void initialize(MarmotCore marmot, RecordSchema intermediateSchema) {
		for ( ValueAggregate aggr: m_aggrs ) {
			aggr.initializeWithIntermediate(intermediateSchema);
		}
		
		setInitialized(marmot, intermediateSchema, intermediateSchema);
	}
	
	public ValueAggregate[] getValueAggregateAll() {
		return m_aggrs;
	}

	@Override
	protected void reduce(Record accum, Record record, int index) {
		if ( index == 0 ) {
			for ( int i =0; i < m_aggrs.length; ++i ) {
				accum.setAll(record.getAll());
			}
		}
		else {
			for ( int i =0; i < m_aggrs.length; ++i ) {
				try {
					ValueAggregate aggr = m_aggrs[i];
					aggr.aggregate(accum, record);
				}
				catch ( Exception e ) {
					System.out.println("***************" + e);
				}
			}
		}
	}
	
	@Override
	public String toString() {
		String aggrsStr = FStream.of(m_aggrs)
								.map(ValueAggregate::toString)
								.join(",");
		return String.format("combine_intermediate[%s]", aggrsStr);
	}

	public static VARIntermediateReducer fromProto(VARIntermediateCombinerProto proto) {
		ValueAggregate[] aggrs = proto.getAggregatorsList()
										.stream()
										.map(ValueAggregates::fromProto)
										.toArray(sz -> new ValueAggregate[sz]);
		return new VARIntermediateReducer(aggrs);
	}
	
	@Override
	public VARIntermediateCombinerProto toProto() {
		return FStream.of(m_aggrs)
						.map(ValueAggregates::toProto)
						.fold(VARIntermediateCombinerProto.newBuilder(),
								(b,aggr) -> b.addAggregators(aggr))
						.build();
	}
}