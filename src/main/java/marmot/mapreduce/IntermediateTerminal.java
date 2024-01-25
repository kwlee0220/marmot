package marmot.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import marmot.MarmotInternalException;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.io.RecordWritable;
import marmot.io.mapreduce.seqfile.MarmotFileOutputFormat;
import marmot.optor.support.AbstractRecordSetConsumer;
import marmot.proto.optor.IntermediateTerminalProto;
import marmot.support.DefaultRecord;
import marmot.support.PBSerializable;
import marmot.support.ProgressReportable;
import utils.StopWatch;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class IntermediateTerminal extends AbstractRecordSetConsumer
							implements HdfsWriterTerminal, ProgressReportable,
										MapReduceJobConfigurer, MarmotMRContextAware,
										PBSerializable<IntermediateTerminalProto> {
	private static final NullWritable NULL_KEY = NullWritable.get();
	
	private final Path m_path;
	
	@SuppressWarnings("rawtypes")
	private TaskInputOutputContext m_context;
	private long m_count;
	private volatile RecordSet m_inputRSet;
	
	IntermediateTerminal(Path path) {
		Utilities.checkNotNullArgument(path, "output path is null");
		
		m_path = path;
		setLogger(LoggerFactory.getLogger(IntermediateTerminal.class));
	}

	@Override
	public Path getOutputPath() {
		return m_path;
	}

	@Override
	public void configure(Job job) {
		checkInitialized();
		
		FileOutputFormat.setOutputPath(job, m_path);
		
		RecordSchema inputSchema = getInputRecordSchema();
		MarmotFileOutputFormat.setRecordSchema(job.getConfiguration(), inputSchema);

		job.setOutputFormatClass(LazyOutputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, MarmotFileOutputFormat.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(RecordWritable.class);
	}

	@Override
	public void setMarmotMRContext(MarmotMRContext context) {
		m_context = context.getHadoopContext();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void consume(RecordSet rset) {
		checkInitialized();
		Preconditions.checkState(m_context != null, "not in a Map-Reduce context");
		
		RecordWritable value = RecordWritable.from(rset.getRecordSchema());
		
		m_inputRSet = rset;
		m_count = 0;
		Record record = DefaultRecord.of(rset.getRecordSchema());

		try {
			while ( rset.next(record) ) {
				value.loadFrom(record);
				m_context.write(NULL_KEY, value);
				
				++m_count;
			}
		}
		catch ( Exception e ) {
			String msg = String.format("fails to write mapper output: record=%s", record);
			getLogger().error(msg, e);
			throw new MarmotInternalException(msg, e);
		}
		finally {
			m_inputRSet = null;
			rset.closeQuietly();
		}
	}

	@Override
	public void reportProgress(Logger logger, StopWatch elapsed) {
		RecordSet rset = m_inputRSet;
		if ( rset != null && rset instanceof ProgressReportable ) {
			((ProgressReportable)rset).reportProgress(logger, elapsed);
		}
		
		long velo = Math.round(m_count / elapsed.getElapsedInFloatingSeconds());
		logger.info("report: {}, count={} elapsed={} velo={}/s",
							this, m_count, elapsed.getElapsedMillisString(), velo);
	}
	
	@Override
	public String toString() {
		return String.format("intermediate_writer[path=%s]", m_path);
	}

	public static IntermediateTerminal fromProto(IntermediateTerminalProto proto) {
		return new IntermediateTerminal(new Path(proto.getPath()));
	}

	@Override
	public IntermediateTerminalProto toProto() {
		return IntermediateTerminalProto.newBuilder()
										.setPath(m_path.toString())
										.build();
	}

	@SuppressWarnings("rawtypes")
	public static class Session {
		private final TaskInputOutputContext m_context;
		private final RecordWritable m_value;
		
		Session(TaskInputOutputContext context, RecordWritable value) {
			m_context = context;
			m_value = value;
		}
	}
}
