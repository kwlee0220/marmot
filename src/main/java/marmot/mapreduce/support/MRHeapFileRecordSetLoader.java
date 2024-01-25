package marmot.mapreduce.support;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import utils.Utilities;
import utils.func.UncheckedConsumer;
import utils.func.UncheckedFunction;
import utils.stream.FStream;

import marmot.MarmotCore;
import marmot.RecordSetException;
import marmot.io.HdfsPath;
import marmot.mapreduce.PlanMRExecutionMode;
import marmot.optor.MapReduceableRecordSetLoader;
import marmot.optor.RecordSetLoader;
import marmot.optor.support.AbstractRecordSetLoader;
import marmot.plan.LoadOptions;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class MRHeapFileRecordSetLoader extends AbstractRecordSetLoader
												implements MapReduceableRecordSetLoader {
	protected final List<Path> m_pathList;
	protected final LoadOptions m_options;
	protected final int m_nsplitsPerBlock;

	@SuppressWarnings("rawtypes")
	protected abstract Class<? extends InputFormat> getInputFormatClass();

	protected MRHeapFileRecordSetLoader(Iterable<String> pathList, LoadOptions opts) {
		Utilities.checkNotNullArgument(pathList, "pathList is null");
		Utilities.checkNotNullArgument(opts, "LoadOptions is null");
		
		m_pathList = FStream.from(pathList)
							.map(Path::new)
							.fold(ImmutableList.<Path>builder(), (b,p) -> b.add(p))
							.build();
		if ( m_pathList.size() == 0 ) {
			throw new IllegalArgumentException("no target file path");
		}
		
		m_options = opts;
		m_nsplitsPerBlock = opts.splitCount().getOrElse(1);
		
		setLogger(LoggerFactory.getLogger(MRHeapFileRecordSetLoader.class));
	}

	public List<Path> getPathList() {
		return m_pathList;
	}

	@Override
	public String getInputString() {
		return m_pathList.toString();
	}

	/**
	 * MapReduce job을 초기화시킨다.
	 * 일반적으로 초기화 작업은 다음과 과정을 밣는다.
	 * <ol>
	 * 	<li> 입력 파일 포맷 설정: {@link Job#setInputFormatClass(Class)}를 이용하여 MapReduce에서
	 * 			사용할 {@link FileInputFormat}을 설정한다.
	 * 	<li> MapReduce 입력 파일 경로 설정: {@link FileInputFormat#addInputPath(Job, Path)}를
	 * 			이용하여 MapReduce 과정에서 읽어들이 입력 파일들의 경로를 설정한다.
	 * 	<li> 입력 Split 크기 설정: {@link RecordSetLoader}가 {@link InputSplit}의 크기와 관련이 있는 경우
	 * 			적절한 split 크기를 설정한다.
	 * 	<li> Configuration 객체 설정: {@link RecordSetLoader} 수행에 필요한 map/reduce task 과정에서 필요한 정보를
	 * 			{@link Configuration} 객체에 설정한다. 
	 * </ol>
	 * 
	 * @param job	초기화시킬 대상 작업 객체.
	 */
	@Override
	public void configure(Job job) {
		checkInitialized();
		
		Configuration conf = job.getConfiguration();
		try {
			// 입력 파일 포맷 설정
			job.setInputFormatClass(getInputFormatClass());
			
			// MapReduce 입력 파일 경로 설정
			HdfsPath.walkRegularFileTree(conf, m_pathList)
					.map(HdfsPath::getPath)
					.peek(path -> getLogger().debug("input file: {}", path))
					.forEach(UncheckedConsumer.sneakyThrow(path -> FileInputFormat.addInputPath(job, path)));
			
			// 입력 Split 크기 설정
			setSplitSize(getMarmotCore(), job);
		}
		catch ( IOException e ) {
			throw new RecordSetException(e);
		}
	}
	
	public int estimateBlockCount(MarmotCore marmot) {
		FileSystem fs = marmot.getHadoopFileSystem();
		
		long nbytes = HdfsPath.walkRegularFileTree(fs, getPathList())
								.map(UncheckedFunction.sneakyThrow(HdfsPath::getFileStatus))
								.mapToLong(FileStatus::getLen)
								.sum();
		long blkSize = MapReduceableRecordSetLoader.getBlockSize(fs, getPathList());
		return (int)Math.ceil(nbytes / (double)blkSize);
	}
	
	public static PlanMRExecutionMode getExecutionMode(MarmotCore marmot,
														List<Path> pathList) throws IOException {
		FileSystem fs = marmot.getHadoopFileSystem();
		
		long nbytes = HdfsPath.walkRegularFileTree(fs, pathList)
								.map(UncheckedFunction.sneakyThrow(HdfsPath::getFileStatus))
								.mapToLong(FileStatus::getLen)
								.sum();
		long blkSize = MapReduceableRecordSetLoader.getBlockSize(fs, pathList);
		int nblocks = (int)Math.ceil(nbytes / (double)blkSize);
		
		if ( nblocks >= marmot.getlocallyExecutableBlockLimitHard() ) {
			return PlanMRExecutionMode.NON_LOCALLY_EXECUTABLE;
		}
		if ( nblocks <= marmot.getLocallyExecutableBlockLimitSoft() ) {
			return PlanMRExecutionMode.LOCALLY_EXECUTABLE;
		}
		else {
			return PlanMRExecutionMode.CONDITIONALLY_EXECUTABLE;
		}
	}

	@Override
	public PlanMRExecutionMode getExecutionMode(MarmotCore marmot) throws IOException {
		if ( m_nsplitsPerBlock > 1 ) {
			return PlanMRExecutionMode.NON_LOCALLY_EXECUTABLE;
		}
		else {
			int nblocks = estimateBlockCount(marmot);
			if ( nblocks >= marmot.getlocallyExecutableBlockLimitHard() ) {
				return PlanMRExecutionMode.NON_LOCALLY_EXECUTABLE;
			}
			if ( nblocks <= marmot.getLocallyExecutableBlockLimitSoft() ) {
				return PlanMRExecutionMode.LOCALLY_EXECUTABLE;
			}
			else {
				return PlanMRExecutionMode.CONDITIONALLY_EXECUTABLE;
			}
		}
	}
	
	protected int getSplitCountPerBlock() {
		return m_nsplitsPerBlock;
	}
	
	private void setSplitSize(MarmotCore marmot, Job job) throws IOException {
		FileSystem fs = marmot.getHadoopFileSystem();
		
		long blkSize = MapReduceableRecordSetLoader.getBlockSize(fs, m_pathList);
		if ( m_nsplitsPerBlock > 1 ) {
			long splitSize = blkSize / m_nsplitsPerBlock;
			FileInputFormat.setMaxInputSplitSize(job, splitSize);
		}
	}
}
