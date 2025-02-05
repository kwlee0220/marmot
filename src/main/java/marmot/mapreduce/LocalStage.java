package marmot.mapreduce;

import java.util.List;
import java.util.concurrent.CancellationException;

import javax.annotation.concurrent.GuardedBy;

import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;

import utils.RuntimeInterruptedException;
import utils.async.CancellableWork;
import utils.async.Guard;
import utils.async.GuardedConsumer;
import utils.async.GuardedSupplier;
import utils.func.FOption;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.exec.MarmotExecutionException;
import marmot.io.HdfsPath;
import marmot.mapreduce.MultiJobPlanExecution.StageContext;
import marmot.optor.RecordSetConsumer;
import marmot.optor.RecordSetFunction;
import marmot.optor.RecordSetLoader;
import marmot.optor.RecordSetOperator;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class LocalStage extends Stage implements CancellableWork {
	private final MarmotCore m_marmot;
	private final StageContext m_context;
	
	private final RecordSetLoader m_loader;
	private final RecordSetConsumer m_consumer;
	private List<RecordSetOperator> m_optors;
	private final FOption<HdfsPath> m_outputPath;
	
	private final Guard m_guard = Guard.create();
	@GuardedBy("m_guard") private RecordSet m_consumerInput = null;
	
	LocalStage(MarmotCore marmot, StageContext ctx, List<RecordSetOperator> optors) {
		m_marmot = marmot;
		m_context = ctx;
		
		m_optors = Lists.newArrayList(optors);
		m_optors.stream()
				.forEach(optor -> StageWorkspaceAware.set(optor, m_context.m_stageWorkspace));

		RecordSetOperator first = m_optors.remove(0);
		if ( !(first instanceof RecordSetLoader) ) {
			throw new MarmotExecutionException("the first operator should be a RecordSetLoader: "
											+ "first=" + first);
		}
		m_loader = (RecordSetLoader)first;
		
		RecordSetOperator last = m_optors.remove(m_optors.size()-1);
		if ( !(last instanceof RecordSetConsumer) ) {
			throw new MarmotExecutionException("the last operator should be a RecordSetConsumer: "
											+ "last=" + last);
		}
		if ( last instanceof HdfsWriterTerminal ) {
			m_outputPath = FOption.ofNullable(((HdfsWriterTerminal)last).getOutputPath())
									.map(p -> HdfsPath.of(m_marmot.getHadoopFileSystem(), p));
		}
		else {
			m_outputPath = FOption.empty();
		}
		m_consumer = (RecordSetConsumer)last;
		
		RecordSetOperator bad = m_optors.stream()
										.filter(o -> !(o instanceof RecordSetFunction))
										.findFirst()
										.orElse(null);
		if ( bad != null ) {
			throw new IllegalArgumentException("intermediate RecordSetOperator should be "
												+ "a RecordSetFunction: optor=" + bad);
		}
	}

	@Override
	public String getId() {
		return m_context.m_stageId;
	}

	@Override
	public FOption<HdfsPath> getOutputPath() {
		return m_outputPath;
	}

	@Override
	public Path getWorkspace() {
		return m_context.m_stageWorkspace;
	}

	@Override
	public RecordSchema getOutputRecordSchema() {
		return m_consumer.getOutputRecordSchema();
	}

	@Override
	protected Void executeWork() throws CancellationException, Exception {
		RecordSet rset = null;
		try {
			rset = m_loader.load();
			if ( isCancelRequested() ) {
				throw new CancellationException();
			}
			
			while ( !m_optors.isEmpty() ) {
				RecordSetFunction functor = (RecordSetFunction)m_optors.remove(0);
				if ( !functor.isInitialized() ) {
					functor.initialize(m_marmot, rset.getRecordSchema());
				}
				
				rset = functor.apply(rset);
				if ( isCancelRequested() ) {
					throw new CancellationException();
				}
			}
			
			m_consumer.initialize(m_marmot, rset.getRecordSchema());
			
			GuardedConsumer.<RecordSet>from(m_guard, rs -> m_consumerInput = rs).accept(rset);
			m_consumer.consume(m_consumerInput);
			
			return null;
		}
		finally {
			if ( rset != null ) {
				rset.closeQuietly();
			}
		}
	}
	
	@Override
	public boolean cancelWork() {
		try {
			RecordSet input = GuardedSupplier.from(m_guard, () -> m_consumerInput)
											.preCondition(() -> m_consumerInput != null || !isRunning())
											.get();
			if ( isRunning() ) {
				input.closeQuietly();
			}
			
			return true;
		}
		catch ( RuntimeInterruptedException e ) {
			return false;
		}
	}
	
	@Override
	public String toString() {
		return String.format("%s[%s]: in=%s, out=%s", m_context.m_stageId, getState(),
							""+m_loader, m_outputPath);
	}
}
