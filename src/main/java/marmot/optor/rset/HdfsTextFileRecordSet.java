package marmot.optor.rset;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.io.HdfsPath;
import marmot.rset.AbstractRecordSet;
import marmot.rset.ConcatedRecordSet;
import marmot.support.HadoopUtils;
import marmot.support.ProgressReportable;
import marmot.type.DataType;
import utils.StopWatch;
import utils.Utilities;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class HdfsTextFileRecordSet extends ConcatedRecordSet implements ProgressReportable {
	private static final String CHARSET = "UTF-8";
	public static final RecordSchema SCHEMA = RecordSchema.builder()
														.addColumn("text", DataType.STRING)
														.build();
	private List<HdfsPath> m_files;
	private int m_fileIdx = -1;
	
	public static HdfsTextFileRecordSet of(FileSystem fs, List<Path> start) {
		Utilities.checkNotNullArgument(fs, "FileSystem is null");
		Utilities.checkNotNullArgument(start, "start path is null");
		
		List<HdfsPath> hdfsPathes = FStream.from(start)
											.map(path -> HdfsPath.of(fs, path))
											.toList();
		return of(hdfsPathes);
	}
	
	public static HdfsTextFileRecordSet of(HdfsPath... start) {
		Utilities.checkNotNullArguments(start, "start path is null");
		
		return of(Arrays.asList(start));
	}
	
	public static HdfsTextFileRecordSet of(List<HdfsPath> start) {
		return new HdfsTextFileRecordSet(start);
	}
	
	private HdfsTextFileRecordSet(List<HdfsPath> start) {
		Utilities.checkNotNullArgument(start, "start path is null");
		
		setLogger(LoggerFactory.getLogger(HdfsTextFileRecordSet.class));
		
		m_files = HdfsPath.walkRegularFileTree(start).toList();
		getLogger().debug("{}", this);
	}

	@Override
	protected RecordSet loadNext() {
		if ( m_fileIdx < m_files.size()-1 ) {
			HdfsPath path = m_files.get(++m_fileIdx);
			getLogger().debug("loading text file: path={} (%d/%d)",
								path, m_fileIdx+1, m_files.size());
			return new SingleFileRecordSet(path);
		}
		else {
			return null;
		}
	}

	@Override
	public RecordSchema getRecordSchema() {
		return SCHEMA;
	}
	
	@Override
	public void reportProgress(Logger logger, StopWatch elapsed) {
		logger.info("report: {}", this);
	}
	
	@Override
	public String toString() {
		String currentStr = "";
		if ( m_fileIdx >= 0 && m_fileIdx < m_files.size() ) {
			currentStr = String.format(", current=%s", m_files.get(m_fileIdx));
		}
		else if ( m_fileIdx < 0 ) {
			currentStr = "not_started";
		}
		else {
			currentStr = "end-of-stream";
		}
		
		return String.format("load_textfiles(%d/%d)%s", m_fileIdx+1, m_files.size(),
								currentStr);
	}
	
	private static class SingleFileRecordSet extends AbstractRecordSet {
		private final HdfsPath m_path;
		private final BufferedReader m_reader;
		
		private SingleFileRecordSet(HdfsPath path) {
			try {
				m_path = path;
				
				InputStream is = path.open();
				
				File file = new File(path.getPath().toString());
				String ext = FilenameUtils.getExtension(file.getAbsolutePath());
				if ( ext.length() > 0 && HadoopUtils.isValidCompressionCodecName(ext)) {
					CompressionCodec codec = HadoopUtils.getCompressionCodecByName(path.getConf(), ext);
					Decompressor decomp = codec.createDecompressor();
					is = codec.createInputStream(is, decomp);
				}
				
				m_reader =  new BufferedReader(new InputStreamReader(is, CHARSET));
			}
			catch ( Exception e ) {
				throw new RecordSetException(e);
			}
		}

		@Override
		protected void closeInGuard() throws Exception {
			m_reader.close();
		}

		@Override
		public RecordSchema getRecordSchema() {
			return SCHEMA;
		}
		
		@Override
		public boolean next(Record output) {
			try {
				String line = m_reader.readLine();
				if ( line != null ) {
					output.set(0, line);
					
					return true;
				}
				else {
					return false;
				}
			}
			catch ( IOException e ) {
				throw new RecordSetException(e);
			}
		}
		
		@Override
		public String toString() {
			return "hdfs_textfile_rset(" + m_path + ")";
		}
	}
}
