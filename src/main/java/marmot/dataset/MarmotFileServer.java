package marmot.dataset;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.MarmotCore;
import marmot.RecordSet;
import marmot.io.HdfsPath;
import marmot.io.MarmotFileException;
import marmot.io.MarmotSequenceFile;
import marmot.optor.LoadMarmotFile;
import marmot.optor.StoreAsHeapfile;
import marmot.support.HadoopUtils;
import utils.StopWatch;
import utils.UnitUtils;
import utils.Utilities;
import utils.func.FOption;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotFileServer {
	private final static Logger s_logger = LoggerFactory.getLogger(MarmotFileServer.class);
	
	private final MarmotCore m_marmot;
	private final Configuration m_conf;
	private final FileSystem m_fs;
	
	public MarmotFileServer(MarmotCore marmot, Configuration conf) {
		m_marmot = marmot;
		m_conf = conf;
		m_fs = HdfsPath.getFileSystem(conf);
	}

	/**
	 * 주어진 경로에 해당하는 MarmotFile에 저장된 레코드 세트를 읽는다.
	 * <p>
	 * 경로는 원격 MarmotServer 내에 위치한 파일의 경로명이다.
	 * 
	 * @param path	대상 파일 경로.
	 * @return	파일에 기록된 레코드 세트. 
	 */
	public RecordSet readMarmotFile(Path path) {
		Utilities.checkNotNullArgument(path, "MarmotFile path is null");
		
		try {
			List<MarmotSequenceFile> files = HdfsPath.of(m_fs, path)
												.walkRegularFileTree()
												.map(MarmotSequenceFile::of)
												.toList();
			return LoadMarmotFile.load(files);
		}
		catch ( IOException e ) {
			throw new MarmotFileException("fails to read MarmotFile: path=" + path, e);
		}
	}
	
	/**
	 * 주어진 레코드 세트를 원격 MarmotServer 내의 주어진 위치에 저장한다.
	 * <p>
	 * 경로는 원격 MarmotServer 내에 위치한 파일의 경로명이다.
	 * 
	 * @param path	생성될 파일의 경로.
	 * @param rset	저장할 레코드 세트.
	 */
	public void writeMarmotFile(Path path, RecordSet rset) {
		Utilities.checkNotNullArgument(path, "MarmotFile path is null");
		Utilities.checkNotNullArgument(rset, "RecordSet is null");
		
		StoreAsHeapfile.store(m_marmot, path, rset);
	}

	public boolean existsMarmotFile(String path) {
		Utilities.checkNotNullArgument(path, "path is null");

		return HdfsPath.of(m_conf, new Path(path)).exists();
	}
	
	public FSDataOutputStream createFile(Path path, long blockSize) throws IOException {
		int bufferSz = m_conf.getInt(
			                    CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
			                    CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);
		return m_fs.create(path, true, bufferSz, m_fs.getDefaultReplication(path), blockSize);
	}

	/**
	 * 주어진 경로에 해당하는 MarmotFile을 삭제한다.
	 * <p>
	 * 경로는 원격 MarmotServer 내에 위치한 파일의 경로명이다.
	 * 
	 * @param path	대상 파일 경로.
	 * @return	 파일 삭제 여부.
	 */
	public boolean deleteFile(Path path) {
		Utilities.checkNotNullArgument(path, "path is null");
		
		return HdfsPath.of(m_conf, path).delete();
	}
	
	public void deleteFileUpward(Path path) {
		HdfsPath.of(m_conf, path).deleteUpward();
	}
	
	/**
	 * 파일의 경로명을 변경시킨다.
	 * 
	 * @param srcPath	변경 전 경로명
	 * @param tarPath	변경 후 경로명
	 * @return	변경 성공 여부.
	 */
	public boolean renameFile(String srcPath, String tarPath) {
		Utilities.checkNotNullArgument(srcPath, "source path is null");
		Utilities.checkNotNullArgument(tarPath, "target path is null");
		
		try {
			HdfsPath.of(m_conf, new Path(srcPath))
					.moveTo(HdfsPath.of(m_conf, new Path(tarPath)));
			return true;
		}
		catch ( Exception e ) {
			return false;
		}
	}

	public long copyToHdfsFile(String dest, InputStream is, FOption<Long> blockSize,
								FOption<String> codecName) throws IOException {
		StopWatch watch = StopWatch.start();
		
		HdfsPath path = HdfsPath.of(m_conf, new Path(dest));
		long blkSz = blockSize.getOrElse(m_marmot.getDefaultTextBlockSize());
		
		CompressionCodec codec = null;
		if ( codecName.isPresent() ) {
			codec = HadoopUtils.getCompressionCodecByName(m_conf, codecName.get());
			path = HdfsPath.of(m_conf, new Path(path.toString() + codec.getDefaultExtension()));
		}
		try ( FSDataOutputStream fsos = path.create(true, blkSz);
				OutputStream out = (codec != null) ? HadoopUtils.toCompressionStream(fsos, codec) : fsos; ) {
			s_logger.debug("start uploading: dest={}", path);
			
			long nbytes = IOUtils.transfer(is, out, 16 * 1024);
			
			watch.stop();
			
			String veloStr = UnitUtils.toByteSizeString(Math.round(nbytes / watch.getElapsedInFloatingSeconds()));
			s_logger.debug("uploaded: dest={}, elapsed={}, velo={}/s",
							path, watch.getElapsedSecondString(), veloStr);
			
			return nbytes;
		}
		catch ( Exception e ) {
			throw e;
		}
	}

	public long getBlockSize(String path) {
		Utilities.checkNotNullArgument(path, "path is null");

		try {
			return HdfsPath.of(m_fs, new Path(path)).getFileStatus().getBlockSize();
		}
		catch ( Throwable e ) {
			throw new MarmotFileException(e);
		}
	}
}
