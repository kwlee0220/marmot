package marmot.optor;

import java.util.Map;
import java.util.concurrent.CancellationException;

import javax.annotation.Nullable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.dataset.GeometryColumnInfo;
import marmot.io.HdfsPath;
import marmot.io.MarmotFileWriteOptions;
import marmot.io.MarmotSequenceFile;
import marmot.io.mapreduce.seqfile.MarmotFileOutputFormat;
import marmot.mapreduce.HdfsWriterTerminal;
import marmot.mapreduce.MapReduceJobConfigurer;
import marmot.mapreduce.MarmotMRContext;
import marmot.mapreduce.MarmotMRContextAware;
import marmot.optor.support.AbstractRecordSetConsumer;
import marmot.proto.optor.StoreAsHeapfileProto;
import marmot.support.PBSerializable;
import marmot.support.ProgressReportable;
import utils.StopWatch;
import utils.Throwables;
import utils.Utilities;


/**
 * {@code StoreAsHeapfile} 연산은 주어진 레코드 세트를 지정된 HDFS 테스트 파일에 저장하는 작업을 수행한다.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StoreAsHeapfile extends AbstractRecordSetConsumer
							implements HdfsWriterTerminal, MapReduceJobConfigurer, MarmotMRContextAware,
										ProgressReportable, PBSerializable<StoreAsHeapfileProto> {
	private final Path m_rootPath;
	private final MarmotFileWriteOptions m_opts;
	@Nullable private GeometryColumnInfo m_gcInfo = null;

	private Path m_outputPath;				// 실제 저장될 파일의 경
	private volatile RecordSet m_inputRSet;	// 저장에 사용되고 있는 RecordSet
	@Nullable private volatile MarmotSequenceFile.Store m_store = null;
	private volatile StopWatch m_watch;
	
	private boolean m_isConsuming;
	protected long m_elapsed;
	private boolean m_finalProgressReported = false;
	
	public StoreAsHeapfile(Path path) {
		this(path, MarmotFileWriteOptions.DEFAULT);
	}
	
	public StoreAsHeapfile(Path path, MarmotFileWriteOptions opts) {
		Utilities.checkNotNullArgument(path, "target heapfile path");
		Utilities.checkNotNullArgument(opts, "MarmotFileWriteOptions");
		
		m_rootPath = path;
		m_opts = opts;
		
		m_outputPath = m_rootPath;
		setLogger(LoggerFactory.getLogger(StoreAsHeapfile.class));
	}
	
	@Override
	public void setMarmotMRContext(MarmotMRContext context) {
		// MapReduce를 통해 수행되는 경우
		String name = String.format("%05d", context.getTaskOrdinal());
		m_outputPath = new Path(m_rootPath, name);
	}

	@Override
	public Path getOutputPath() {
		return m_outputPath;
	}
	
	@Override
	public void configure(Job job) {
		checkInitialized();
		
		RecordSchema inputSchema = getInputRecordSchema();
		MarmotFileOutputFormat.setRecordSchema(job.getConfiguration(), inputSchema);
		m_opts.metaData().ifPresent(meta -> {
			for ( Map.Entry<String, String> prop: meta.entrySet() ) {
				job.getConfiguration().set(prop.getKey(), prop.getValue());
			}
		});

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		job.setOutputValueClass(NullWritable.class);
	}
	
	@Override
	public void consume(RecordSet rset) {
		try {
			checkInitialized();
			
			m_watch = StopWatch.start();
			m_isConsuming = true;

			HdfsPath path = HdfsPath.of(m_marmot.getHadoopFileSystem(), m_outputPath);
			MarmotFileWriteOptions writeOpts = m_opts.blockSize(getBlockSize(m_marmot));

			m_inputRSet = rset;
			m_store = MarmotSequenceFile.store(path, rset, m_gcInfo, writeOpts);
			m_store.setOperatorName(getClass().getSimpleName());
			m_store.call();
		}
		catch ( CancellationException e ) { }
		catch ( Exception e ) {
			throw new RecordSetException(Throwables.unwrapThrowable(e));
		}
		finally {
			m_isConsuming = false;
			m_watch.stop();
		}
	}
	
	@Override
	public void reportProgress(Logger logger, StopWatch elapsed) {
		if ( m_isConsuming || !m_finalProgressReported ) {
			if ( m_store != null ) {
				m_store.reportProgress(logger, elapsed);
				if ( !m_isConsuming ) {
					m_finalProgressReported = true;
				}
			}
		}
	}
	
	@Override
	public String toString() {
		return String.format("%s: path=%s", getClass().getSimpleName(), m_outputPath);
	}
	
	public static void store(MarmotCore marmot, Path path, RecordSet rset) {
		StoreAsHeapfile store = new StoreAsHeapfile(path, MarmotFileWriteOptions.DEFAULT);
		store.initialize(marmot, rset.getRecordSchema());
		store.consume(rset);
	}

	public static StoreAsHeapfile fromProto(StoreAsHeapfileProto proto) {
		MarmotFileWriteOptions opts = MarmotFileWriteOptions.fromProto(proto.getWriteOptions());
		return new StoreAsHeapfile(new Path(proto.getPath()), opts);
	}

	@Override
	public StoreAsHeapfileProto toProto() {
		return StoreAsHeapfileProto.newBuilder()
									.setPath(m_rootPath.toString())
									.setWriteOptions(m_opts.toProto())
									.build();
	}
	
	private long getBlockSize(MarmotCore marmot) {
		return m_opts.blockSize().getOrElse(marmot.getDefaultMarmotBlockSize());
	}
}
