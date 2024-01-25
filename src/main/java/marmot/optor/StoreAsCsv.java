package marmot.optor;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.commons.csv.CSVFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.externio.csv.CsvRecordWriter;
import marmot.io.HdfsPath;
import marmot.mapreduce.HdfsWriterTerminal;
import marmot.mapreduce.MapReduceJobConfigurer;
import marmot.mapreduce.MarmotMRContext;
import marmot.mapreduce.MarmotMRContexts;
import marmot.optor.support.AbstractRecordSetConsumer;
import marmot.proto.optor.StoreAsCsvProto;
import marmot.support.HadoopUtils;
import marmot.support.PBSerializable;
import utils.Utilities;
import utils.func.FOption;


/**
 * {@code StoreAsCsv} 연산은 주어진 레코드 세트를 지정된 HDFS 테스트 파일에 저장하는
 * 작업을 수행한다. 이때 각 레코드는 하나의 라인으로 구성되고, 레코드 내의 컬럼은
 * 지정된 문자로 분리된다.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StoreAsCsv extends AbstractRecordSetConsumer
						implements HdfsWriterTerminal, MapReduceJobConfigurer,
									PBSerializable<StoreAsCsvProto> {
	private final Path m_path;
	private final StoreAsCsvOptions m_options;
	private CSVFormat m_format;
	
	private StoreAsCsv(Path path, StoreAsCsvOptions opts) {
		Utilities.checkNotNullArgument(path, "path is null");
		
		m_path = path;
		m_options = opts;
		m_format = CSVFormat.DEFAULT.withDelimiter(opts.delimiter())
									.withQuote(null);
		opts.quote().ifPresent(c -> m_format = m_format.withQuote(c));
		opts.escape().ifPresent(c -> m_format = m_format.withEscape(c));
	}

	@Override
	public Path getOutputPath() {
		return m_path;
	}
	
	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		HdfsPath path = HdfsPath.of(marmot.getHadoopFileSystem(), m_path);
		if ( path.exists() ) {
			path.delete();
		}
		
		setInitialized(marmot, inputSchema);
	}
	
	@Override
	public void configure(Job job) {
		checkInitialized();
		
		job.getConfiguration().set("delimiter", ""+m_format.getDelimiter());

		job.setOutputFormatClass(NullOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);
	}

	@Override
	public void consume(RecordSet rset) {
		checkInitialized();
		
		try ( BufferedWriter bw = new BufferedWriter(newCsvWriter(m_marmot.getHadoopConfiguration()));
				CsvRecordWriter writer = CsvRecordWriter.get(bw, rset.getRecordSchema(),
															m_options.getCsvOptions()) ) {
			writer.write(rset);
		}
		catch ( IOException e ) {
			throw new RecordSetException(e);
		}
	}
	
	@Override
	public String toString() {
		return String.format("store_csv[path=%s,delim=%s]", ""+m_path, ""+m_format.getDelimiter());
	}
	
	private Writer newCsvWriter(Configuration conf) throws IOException {
		Configuration hconf = m_options.blockSize()
										.map(sz -> {
											Configuration xconf = new Configuration(conf);
											xconf.set("dfs.block.size", ""+sz);
											return xconf;
										}).getOrElse(conf);
		FileSystem fs = FileSystem.get(hconf);

		HdfsPath partPath = HdfsPath.of(fs, m_path);
		MarmotMRContext context = MarmotMRContexts.get().getOrNull();
		if ( context != null && context.getNumTasks() > 1 ) {
			String name = String.format("%05d", context.getTaskOrdinal());
			partPath = partPath.child(name);
		}
		
		FOption<CompressionCodec> ocodec = m_options.compressionCodecName()
													.map(name -> HadoopUtils.getCompressionCodecByName(hconf, name));
		if ( ocodec.isPresent() ) {
			String fullPath = partPath.toString() + ocodec.get().getDefaultExtension();
			partPath = HdfsPath.of(fs, new Path(fullPath));
		}
		
		FSDataOutputStream fsdos = partPath.create();
		
		// compression codec name이 설정된 경우는 해당 이름에 해당하는 codec을 구해
		// OutputStream을 변형시킨다.
		OutputStream out = ocodec.mapOrThrow(codec -> HadoopUtils.toCompressionStream(fsdos, codec))
								.cast(OutputStream.class)
								.getOrElse(fsdos);
		return m_options.charset()
						.map(cs -> new OutputStreamWriter(out, cs))
						.getOrElse(()-> new OutputStreamWriter(out));
	}

	public static StoreAsCsv fromProto(StoreAsCsvProto proto) {
		StoreAsCsvOptions opts = StoreAsCsvOptions.fromProto(proto.getOptions());
		
		return new StoreAsCsv(new Path(proto.getPath()), opts);
	}

	@Override
	public StoreAsCsvProto toProto() {
		return StoreAsCsvProto.newBuilder()
								.setPath(m_path.toString())
								.setOptions(m_options.toProto())
								.build();
	}
}
