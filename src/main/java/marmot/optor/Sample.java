package marmot.optor;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.optor.support.Filter;
import marmot.plan.PredicateOptions;
import marmot.proto.optor.SampleProto;
import marmot.support.PBSerializable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Sample extends Filter<Sample> implements PBSerializable<SampleProto> {
	static final Logger s_logger = LoggerFactory.getLogger(Sample.class);
	
	private final double m_ratio;
	private Random m_random;

	public Sample(double ratio) {
		super(PredicateOptions.DEFAULT);
		
		Preconditions.checkArgument(Double.compare(ratio, 0) > 0,
									"invalid sampling raito: " + ratio);
		
		m_ratio = ratio;
		setLogger(s_logger);
	}
	
	public double getRatio() {
		return m_ratio;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		m_random = new Random(System.currentTimeMillis());
		
		super.initialize(marmot, inputSchema);
	}

	@Override
	public boolean test(Record record) {
		checkInitialized();
		
		double r = m_random.nextFloat();
		return Double.compare(r, m_ratio) <= 0;
	}
	
	@Override
	public String toString() {
		return String.format("%s: ratio=%.2f%%", getClass().getSimpleName(), m_ratio * 100);
	}

	public static Sample fromProto(SampleProto proto) {
		return new Sample(proto.getSampleRatio());
	}

	@Override
	public SampleProto toProto() {
		return SampleProto.newBuilder()
						.setSampleRatio(m_ratio)
						.build();
	}
}