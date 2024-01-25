package marmot.optor;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
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
import marmot.mapreduce.input.whole.WholeFileInputFormat;
import marmot.mapreduce.support.MRHeapFileRecordSetLoader;
import marmot.plan.LoadOptions;
import marmot.proto.optor.LoadWholeFileProto;
import marmot.rset.ConcatedRecordSet;
import marmot.support.DefaultRecord;
import marmot.support.PBSerializable;
import marmot.support.ProgressReportable;
import utils.StopWatch;
import utils.io.IOUtils;
import utils.stream.FStream;

/**
 * {@code LoadMarmotFile}연산은 지정된 HDFS 파일 (또는 복수개의 HDFS 파일들)을 읽어 저장된
 * 레코드를 읽어 레코드 세트로 생성하는 작업을 수행한다.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LoadWholeFile extends MRHeapFileRecordSetLoader
							implements PBSerializable<LoadWholeFileProto> {
	static final Logger s_logger = LoggerFactory.getLogger(LoadWholeFile.class);
	
	private List<HdfsPath> m_files;	// set when initialized
	
	public static LoadWholeFile from(Iterable<String> pathList) {
		return new LoadWholeFile(pathList, LoadOptions.DEFAULT);
	}
	
	public static LoadWholeFile from(Path path) {
		return from(path.toString());
	}
	
	public static LoadWholeFile from(String path) {
		return new LoadWholeFile(Collections.singleton(path), LoadOptions.DEFAULT);
	}
	
	public LoadWholeFile(String path, LoadOptions opts) {
		this(Collections.singleton(path), opts);
	}
	
	LoadWholeFile(Iterable<String> pathList, LoadOptions opts) {
		super(pathList, opts);
		
		setLogger(s_logger);
	}

	@Override
	public void initialize(MarmotCore marmot) {
		try {
			m_files = HdfsPath.walkRegularFileTree(marmot.getHadoopFileSystem(), getPathList())
								.toList();
			if ( m_files.size() == 0 ) {
				// 이전 Map/Reduce 단계에서 레코드세트가 생성되지 않은 경우, Plan 수행을 중단시킨다.
				throw new RecordSetNotExistsException("no Marmot files to load: pathList="
														+ getPathList());
			}

			RecordSchema outSchema = WholeFileInputFormat.SCHEMA;
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

	@SuppressWarnings("rawtypes")
	@Override
	public Class<? extends FileInputFormat> getInputFormatClass() {
		return WholeFileInputFormat.class;
	}

//	@Override
//	public PlanMRExecutionMode getExecutionMode(MarmotServer marmot) throws IOException {
//		return PlanMRExecutionMode.NON_LOCALLY_EXECUTABLE;
//	}

	@Override
	public RecordSet load() {
		checkInitialized();

		return new RecordSetImpl(m_files);
	}
	
	public static RecordSet load(List<HdfsPath> files) {
		return new RecordSetImpl(files);
	}
	
	@Override
	public String toString() {
		return String.format("load_marmot%s", getPathList());
	}
	
	public static RecordSet load(MarmotCore marmot, String path) {
		LoadWholeFile load = new LoadWholeFile(Arrays.asList(path), LoadOptions.DEFAULT);
		load.initialize(marmot);
		return load.load();
	}
	
	public static Record readARecord(MarmotCore marmot, String path) {
		try ( RecordSet rset = load(marmot, path) ) {
			return rset.fstream().findFirst().get();
		}
	}

	public static LoadWholeFile fromProto(LoadWholeFileProto proto) {
		LoadOptions opts = LoadOptions.fromProto(proto.getOptions());
		return new LoadWholeFile(proto.getPathsList(), opts);
	}

	@Override
	public LoadWholeFileProto toProto() {
		return LoadWholeFileProto.newBuilder()
								.addAllPaths(FStream.from(m_pathList)
													.map(Path::toString)
													.toList())
								.setOptions(m_options.toProto())
								.build();
	}
	
	private static class RecordSetImpl extends ConcatedRecordSet implements ProgressReportable {
		private List<HdfsPath> m_files;
		private RecordSchema m_schema;
		private int m_fileIdx = 0;
		
		RecordSetImpl(List<HdfsPath> starts) {
			setLogger(LoggerFactory.getLogger(RecordSetImpl.class));

			m_files = starts;
			m_schema = WholeFileInputFormat.SCHEMA;
			
			getLogger().debug("loading whole files: path={}, nfiles={}", starts, m_files.size());
		}

		@Override
		protected RecordSet loadNext() {
			if ( m_fileIdx >= m_files.size() ) {
				return null;
			}
			
			HdfsPath path = m_files.get(m_fileIdx++);

			try ( FSDataInputStream is = path.open() ) {
				byte[] bytes = IOUtils.toBytes(is);
				
				Record record = DefaultRecord.of(m_schema);
				record.set(0, path.toString());
				record.set(1, bytes);
				
				return RecordSet.of(record);
			}
			catch ( IOException e ) {
				throw new RecordSetException(e);
			}
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
			return String.format("load_whole_files%s: (%d/%d)", m_files, m_fileIdx, m_files.size());
		}
	}
}
