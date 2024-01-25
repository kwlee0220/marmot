package marmot.optor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.RecordSetNotExistsException;
import marmot.io.HdfsPath;
import marmot.io.MarmotSequenceFile;
import marmot.io.mapreduce.seqfile.MarmotFileInputFormat;
import marmot.mapreduce.input.fixed.FixedMapperFileInputFormat;
import marmot.mapreduce.support.MRHeapFileRecordSetLoader;
import marmot.plan.LoadOptions;
import marmot.proto.optor.LoadMarmotFileProto;
import marmot.rset.ConcatedRecordSet;
import marmot.support.PBSerializable;
import marmot.support.ProgressReportable;
import utils.StopWatch;
import utils.stream.FStream;

/**
 * {@code LoadMarmotFile}연산은 지정된 HDFS 파일 (또는 복수개의 HDFS 파일들)을 읽어 저장된
 * 레코드를 읽어 레코드 세트로 생성하는 작업을 수행한다.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LoadMarmotFile extends MRHeapFileRecordSetLoader
							implements PBSerializable<LoadMarmotFileProto> {
	static final Logger s_logger = LoggerFactory.getLogger(LoadMarmotFile.class);
	
	private List<MarmotSequenceFile> m_files;	// set when initialized
	
	public static LoadMarmotFile from(Iterable<String> pathList) {
		return new LoadMarmotFile(pathList, LoadOptions.DEFAULT);
	}
	
	public static LoadMarmotFile from(Path path) {
		return from(path.toString());
	}
	
	public static LoadMarmotFile from(String path) {
		return new LoadMarmotFile(Collections.singleton(path), LoadOptions.DEFAULT);
	}
	
	public LoadMarmotFile(String path, LoadOptions opts) {
		this(Collections.singleton(path), opts);
	}
	
	LoadMarmotFile(Iterable<String> pathList, LoadOptions opts) {
		super(pathList, opts);
		
		setLogger(s_logger);
	}
	
	@Override
	public void configure(Job job) {
		super.configure(job);
		
		int mapperCount = m_options.mapperCount().getOrElse(-1);
		if ( mapperCount >= 0 ) {
			Configuration conf = job.getConfiguration();
			
			int vCoreCount = m_marmot.getClusterMetric().getTotalVCoreCount();
			if ( mapperCount == 0 ) {
				mapperCount = Math.round(vCoreCount * 0.9f);
			}
			else {
				mapperCount = Math.min(mapperCount, vCoreCount);
			}

			String jobId = UUID.randomUUID().toString();
			FixedMapperFileInputFormat.Parameters params
					= new FixedMapperFileInputFormat.Parameters(jobId, mapperCount,
																MarmotFileInputFormat.class.getName());
			FixedMapperFileInputFormat.setParameters(conf, params);
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Class<? extends FileInputFormat> getInputFormatClass() {
		if ( m_options.mapperCount().getOrElse(-1) >= 0 ) {
			return FixedMapperFileInputFormat.class;
		}
		else {
			return MarmotFileInputFormat.class;
		}
	}

	@Override
	public void initialize(MarmotCore marmot) {
		try {
			m_files = HdfsPath.walkRegularFileTree(marmot.getHadoopFileSystem(), getPathList())
								.map(MarmotSequenceFile::of)
								.toList();
			if ( m_files.size() == 0 ) {
				// 이전 Map/Reduce 단계에서 레코드세트가 생성되지 않은 경우, Plan 수행을 중단시킨다.
				throw new RecordSetNotExistsException("no Marmot files to load: pathList="
														+ getPathList());
			}

			RecordSchema outSchema = m_files.get(0).getRecordSchema(); 
			setInitialized(marmot, outSchema);
		}
		catch ( RecordSetException e ) { 
			throw e;
		}
		catch ( Exception e ) {
			throw new RecordSetException("fails to initialize the optor " + this
										+ ", cause=" + e);
		}
	}

	@Override
	public RecordSet load() {
		checkInitialized();

		return new RecordSetImpl(m_files);
	}
	
	public static RecordSet load(List<MarmotSequenceFile> files) {
		return new RecordSetImpl(files);
	}
	
	@Override
	public String toString() {
		return String.format("load_marmot%s", getPathList());
	}
	
	public static RecordSet load(MarmotCore marmot, String path) {
		LoadMarmotFile load = new LoadMarmotFile(Arrays.asList(path), LoadOptions.DEFAULT);
		load.initialize(marmot);
		return load.load();
	}
	
	public static Record readARecord(MarmotCore marmot, Path path) {
		try ( RecordSet rset = load(marmot, path.toString()) ) {
			return rset.fstream().findFirst().get();
		}
	}

	public static LoadMarmotFile fromProto(LoadMarmotFileProto proto) {
		LoadOptions opts = LoadOptions.fromProto(proto.getOptions());
		return new LoadMarmotFile(proto.getPathsList(), opts);
	}

	@Override
	public LoadMarmotFileProto toProto() {
		return LoadMarmotFileProto.newBuilder()
								.addAllPaths(FStream.from(m_pathList)
													.map(Path::toString)
													.toList())
								.setOptions(m_options.toProto())
								.build();
	}
	
	private static class RecordSetImpl extends ConcatedRecordSet implements ProgressReportable {
		private List<MarmotSequenceFile> m_files;
		private RecordSchema m_schema;
		private int m_fileIdx = 0;
		
		RecordSetImpl(List<MarmotSequenceFile> starts) {
			setLogger(LoggerFactory.getLogger(RecordSetImpl.class));

			m_files = starts;
			m_schema = m_files.get(0).getRecordSchema();
			
			getLogger().debug("loading marmot file: path={}, nfiles={}", starts, m_files.size());
		}

		@Override
		protected RecordSet loadNext() {
			return (m_fileIdx < m_files.size()) ? m_files.get(m_fileIdx++).read() : null;
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_schema;
		}
		
		@Override
		public void reportProgress(Logger logger, StopWatch elapsed) {
			logger.info("report: {}", this);
		}
		
		@Override
		public String toString() {
			return String.format("load_marmot%s: (%d/%d)", m_files, m_fileIdx, m_files.size());
		}
	}
}
