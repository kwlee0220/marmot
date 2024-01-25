package marmot.support;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import org.slf4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import marmot.MarmotCore;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSets;
import marmot.exec.MarmotExecutionException;
import marmot.optor.RecordSetConsumer;
import marmot.optor.RecordSetFunction;
import marmot.optor.RecordSetLoader;
import marmot.optor.RecordSetOperator;
import marmot.optor.RecordSetOperatorException;
import marmot.proto.optor.OperatorProto;
import marmot.protobuf.ProtoBufActivator;
import utils.Utilities;
import utils.func.Try;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordSetOperatorChain {
	private static final String INDENT_BLANKS = "   ";
	
	private final MarmotCore m_marmot;
	
	private RecordSchema m_inputSchema;
	private List<RecordSetOperator> m_optors = Lists.newArrayList();
	private RecordSchema m_outputSchema = null;
	
	public static RecordSetOperatorChain from(MarmotCore marmot, RecordSchema inputSchema) {
		Utilities.checkNotNullArgument(marmot, "marmot is null");
		Utilities.checkNotNullArgument(inputSchema, "inputSchema is null");
		
		return new RecordSetOperatorChain(marmot).setInputRecordSchema(inputSchema);
	}
	
	public static RecordSetOperatorChain from(MarmotCore marmot) {
		Utilities.checkNotNullArgument(marmot, "marmot is null");
		
		return new RecordSetOperatorChain(marmot);
	}
	
	public static RecordSetOperatorChain from(MarmotCore marmot, Plan plan,
												RecordSchema inputSchema) {
		Utilities.checkNotNullArgument(marmot, "marmot is null");
		Utilities.checkNotNullArgument(plan, "plan is null");
		Utilities.checkNotNullArgument(inputSchema, "inputSchema is null");
		
		List<RecordSetOperator> optors = FStream.from(plan.toProto().getOperatorsList())
												.map(RecordSetOperatorChain::load)
												.toList();
		return from(marmot, inputSchema).addAll(optors);
	}
	
	public static RecordSetOperatorChain from(MarmotCore marmot, Plan plan) {
		Utilities.checkNotNullArgument(marmot, "marmot is null");
		Utilities.checkNotNullArgument(plan, "plan is null");
		
		List<RecordSetOperator> optors = FStream.from(plan.toProto().getOperatorsList())
												.map(RecordSetOperatorChain::load)
												.toList();
		return from(marmot).addAll(optors);
	}
	
	private RecordSetOperatorChain(MarmotCore marmot) {
		m_marmot = marmot;
	}
	
	public MarmotCore getMarmotServer() {
		return m_marmot;
	}
	
	public RecordSchema getInputRecordSchema() {
		RecordSetLoader loader = getRecordSetLoader();
		if ( loader != null ) {
			return initialize(loader, null);
		}
		else {
			return m_inputSchema;
		}
	}
	
	public RecordSetOperatorChain setInputRecordSchema(RecordSchema schema) {
		m_inputSchema = schema;
		m_outputSchema = null;
		return this;
	}
	
	public RecordSchema getOutputRecordSchema() {
		if ( m_outputSchema == null ) {
			RecordSchema outSchema = m_inputSchema;
			for ( RecordSetOperator optor: m_optors ) {
				outSchema = initialize(optor, outSchema);
			}
			
			m_outputSchema = outSchema;
		}
		
		return m_outputSchema;
	}
	
	public int length() {
		return m_optors.size();
	}
	
	public List<RecordSetOperator> getAll() {
		return Collections.unmodifiableList(m_optors);
	}
	
	public RecordSetOperator get(int index) {
		Preconditions.checkArgument(index >= 0 && index < m_optors.size());

		return m_optors.get(index);
	}
	
	public RecordSetOperator getFirst() {
		if ( m_optors.size() > 0 ) {
			return m_optors.get(0);
		}
		else {
			throw new NoSuchElementException("first");
		}
	}
	
	public RecordSetOperator getLast() {
		if ( m_optors.size() > 0 ) {
			return m_optors.get(m_optors.size()-1);
		}
		else {
			throw new NoSuchElementException("last");
		}
	}
	
	public RecordSetOperator removeFirst() {
		if ( m_optors.size() > 0 ) {
			m_outputSchema = null;	// chain이 변경되었기 때문에, caching된 output-schema를 제거시킨다
			return m_optors.remove(0);
		}
		else {
			throw new NoSuchElementException("first");
		}
	}
	
	public RecordSetLoader getRecordSetLoader() {
		if ( m_optors.size() > 0 ) {
			RecordSetOperator first = m_optors.get(0);
			return ( first instanceof RecordSetLoader ) ? (RecordSetLoader)first : null;
		}
		else {
			return null;
		}
	}
	
	public RecordSetOperatorChain setRecordSetLoader(RecordSetLoader loader) {
		if ( m_optors.size() > 0 && m_optors.get(0) instanceof RecordSetLoader ) {
			m_optors.remove(0);
		}
		
		if ( loader != null ) {
			m_optors.add(0, loader);
		}
		m_outputSchema = null;	// chain이 변경되었기 때문에, caching된 output-schema를 제거시킨다
		
		return this;
	}
	
	public RecordSetConsumer getRecordSetConsumer() {
		if ( m_optors.size() > 0 ) {
			RecordSetOperator last = m_optors.get(m_optors.size()-1);
			return ( last instanceof RecordSetConsumer ) ? (RecordSetConsumer)last : null;
		}
		else {
			return null;
		}
	}
	
	public RecordSetOperatorChain setRecordSetConsumer(RecordSetConsumer consumer) {
		if ( m_optors.size() > 0 && m_optors.get(m_optors.size()-1) instanceof RecordSetConsumer ) {
			m_optors.remove(m_optors.size()-1);
		}
		
		if ( consumer != null ) {
			m_optors.add(consumer);
		}
		m_outputSchema = null;	// chain이 변경되었기 때문에, caching된 output-schema를 제거시킨다
		
		return this;
	}
	
	public RecordSetOperatorChain add(RecordSetOperator optor) {
		m_optors.add(optor);
		m_outputSchema = null;	// chain이 변경되었기 때문에, caching된 output-schema를 제거시킨다
		return this;
	}
	
	public RecordSetOperatorChain add(int index, RecordSetOperator optor) {
		m_optors.add(index, optor);
		m_outputSchema = null;	// chain이 변경되었기 때문에, caching된 output-schema를 제거시킨다
		return this;
	}
	
	public RecordSetOperatorChain addAll(List<RecordSetOperator> optors) {
		m_optors.addAll(optors);
		m_outputSchema = null;	// chain이 변경되었기 때문에, caching된 output-schema를 제거시킨다
		return this;
	}
	
	public RecordSetOperatorChain addAll(RecordSetOperator... optors) {
		return addAll(Arrays.asList(optors));
	}
	
	public RecordSchema initialize() {
		return getOutputRecordSchema();
	}
	
	public RecordSet run() {
		Preconditions.checkState(getRecordSetLoader() != null);
		
		RecordSet rset = null;
		for ( RecordSetOperator optor: m_optors ) {
			rset = run(optor, rset);
		}
		
		return rset;
	}
	
	public RecordSet run(RecordSet input) {
		Utilities.checkNotNullArgument(input, "input is null");
		Preconditions.checkState(getRecordSetLoader() == null);
		Preconditions.checkArgument(getInputRecordSchema().equals(input.getRecordSchema()));
		
		RecordSet rset = input;
		for ( RecordSetOperator optor: m_optors ) {
			rset = run(optor, rset);
		}
		
		return rset;
	}
	
	public FStream<RecordSetOperator> streamOperators() {
		return FStream.from(m_optors);
	}
	
	public Plan toPlan(String planName) {
		return PlanUtils.toPlan(planName, m_optors);
	}
	
	@Override
	public String toString() {
		final String nl = System.lineSeparator();
		StringBuilder builder = new StringBuilder();
		
		builder.append("{input_schema: ");
		if ( m_inputSchema != null ) {
			builder.append(m_inputSchema);
		}
		else {
			builder.append("unknown");
		}
		builder.append("}" + nl);
		
		FStream.from(m_optors)
				.map(RecordSetOperator::toString)
				.forEach(str -> builder.append(str).append(nl));

		Try.run(() -> {
			RecordSchema outSchema = getOutputRecordSchema();
			builder.append("{output_schema: " + outSchema + "}");
		});
		
		return builder.toString();
	}
	
	public void traceLog(Logger logger) {
		int skip = 0;
		String schemaStr;
		if ( m_inputSchema != null ) {
			schemaStr = m_inputSchema.toString();
		}
		else {
			RecordSetLoader loader = getRecordSetLoader();
			if ( loader != null ) {
				logger.info("{}{}", INDENT_BLANKS, loader);
				schemaStr = initialize(loader, null).toString();
				skip = 1;
			}
			else {
				schemaStr = "unknown";
			}
		}
		logger.info("{}{input_schema=[{}]}", INDENT_BLANKS, schemaStr);

		FStream.from(m_optors)
				.drop(skip)
				.map(RecordSetOperator::toString)
				.forEach(str -> logger.info("{}{}", INDENT_BLANKS, str));
				
		Try.run(() -> logger.info("{}{output_schema=[{}]}",
									INDENT_BLANKS, getOutputRecordSchema()));
	}

	private RecordSchema initialize(RecordSetOperator optor, RecordSchema input) {
		try {
			if ( optor instanceof RecordSetFunction ) {
				RecordSetFunction func = (RecordSetFunction)optor;
				if ( !func.isInitialized() ) {
					func.initialize(m_marmot, input);
				}
			}
			else if ( optor instanceof RecordSetLoader ) {
				RecordSetLoader loader = (RecordSetLoader)optor;
				if ( !loader.isInitialized() ) {
					loader.initialize(m_marmot);
				}
			}
			else if ( optor instanceof RecordSetConsumer ) {
				RecordSetConsumer consumer = (RecordSetConsumer)optor;
				if ( !consumer.isInitialized() ) {
					consumer.initialize(m_marmot, input);
				}
			}
		}
		catch ( RecordSetOperatorException e ) {
			throw e;
		}
		catch ( Exception e ) {
			throw new MarmotExecutionException(String.format("op=%s: operator initialization failure, details=%s", optor, e));
		}
		
		return optor.getRecordSchema();
	}

	private RecordSet run(RecordSetOperator optor, RecordSet input) {
		if ( optor instanceof RecordSetFunction ) {
			RecordSetFunction func = (RecordSetFunction)optor;
			if ( !func.isInitialized() ) {
				func.initialize(m_marmot, input.getRecordSchema());
			}
			return func.apply(input);
		}
		else if ( optor instanceof RecordSetLoader ) {
			RecordSetLoader loader = (RecordSetLoader)optor;
			if ( !loader.isInitialized() ) {
				loader.initialize(m_marmot);
			}
			return loader.load();
		}
		else if ( optor instanceof RecordSetConsumer ) {
			RecordSetConsumer consumer = (RecordSetConsumer)optor;
			if ( !consumer.isInitialized() ) {
				consumer.initialize(m_marmot, input.getRecordSchema());
			}
			consumer.consume(input);
			
			return RecordSets.NULL;
		}
		else {
			throw new AssertionError();
		}
	}

	private static RecordSetOperator load(OperatorProto proto) {
		int num = proto.getOperatorCase().getNumber();
		FieldDescriptor desc = OperatorProto.getDescriptor().findFieldByNumber(num);
		Message msg = (Message)proto.getField(desc);
		
		Object activated = ProtoBufActivator.activate(msg);
		if ( activated instanceof RecordSetOperator ) {
			return (RecordSetOperator)activated;
		}
		else {
			throw new MarmotExecutionException(
							"invalid RecordSetOperator Proto: proto=" + proto);
		}
	}
}
