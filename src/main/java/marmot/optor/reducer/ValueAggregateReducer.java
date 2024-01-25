package marmot.optor.reducer;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import marmot.Record;
import marmot.RecordSet;
import marmot.optor.support.RecordLevelTransform;
import marmot.proto.optor.ValueAggregateReducersProto;
import marmot.support.DefaultRecord;
import marmot.support.PBSerializable;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ValueAggregateReducer extends CombineableRecordSetReducer
									implements PBSerializable<ValueAggregateReducersProto> {
	private final ValueAggregate[] m_aggrs;
	
	public static ValueAggregateReducer from(Collection<ValueAggregate> aggregators) {
		return from(aggregators.toArray(new ValueAggregate[aggregators.size()]));
	}
	
	public static ValueAggregateReducer from(ValueAggregate... aggregators) {
		return new ValueAggregateReducer(aggregators);
	}
	
	private ValueAggregateReducer(ValueAggregate... aggregators) {
		m_aggrs = aggregators;
	}
	
	public List<ValueAggregate> getValueAggregators() {
		return Arrays.asList(m_aggrs);
	}

	@Override
	public RecordLevelTransform newIntermediateProducer() {
		return new VARIntermediateProducer(m_aggrs);
	}

	@Override
	public VARIntermediateReducer newIntermediateReducer() {
		return new VARIntermediateReducer(m_aggrs);
	}

	@Override
	public RecordLevelTransform newIntermediateFinalizer() {
		return new VARIntermediateFinalizer(m_aggrs);
	}

	@Override
	protected RecordSet fromEmptyInputRecordSet() {
		Record reduced = DefaultRecord.of(getRecordSchema());
		for ( int i =0; i < m_aggrs.length; ++i ) {
			if ( m_aggrs[i] instanceof Count ) {
				reduced.set(i, 0L);
			}
		}
		
		return RecordSet.of(reduced);
	}

    public static ValueAggregateReducer fromProto(ValueAggregateReducersProto proto) {
        List<ValueAggregate> aggrs = FStream.from(proto.getAggregateList())
                                            .map(ValueAggregates::fromProto)
                                            .toList();
        return ValueAggregateReducer.from(aggrs);
    }

    @Override
    public ValueAggregateReducersProto toProto() {
        return FStream.of(m_aggrs)
                        .map(ValueAggregates::toProto)
                        .fold(ValueAggregateReducersProto.newBuilder(),
                                (b,aggr) -> b.addAggregate(aggr))
                        .build();
    }
	
	@Override
	public String toString() {
		String body = FStream.of(m_aggrs)
							.map(ValueAggregate::toString)
							.join(",");
		return String.format("%s[%s]", ValueAggregateReducer.class, body);
	}
}
