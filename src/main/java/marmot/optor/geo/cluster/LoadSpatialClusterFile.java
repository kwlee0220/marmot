package marmot.optor.geo.cluster;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.geo.CoordinateTransform;
import marmot.io.HdfsPath;
import marmot.io.geo.cluster.SpatialClusterFile;
import marmot.io.geo.cluster.SpatialClusterInfo;
import marmot.io.mapreduce.spcluster.SpatialClusterInputFileFormat;
import marmot.io.mapreduce.spcluster.SpatialClusterInputFileFormat.Parameters;
import marmot.mapreduce.PlanMRExecutionMode;
import marmot.mapreduce.input.fixed.FixedMapperFileInputFormat;
import marmot.optor.MapReduceableRecordSetLoader;
import marmot.optor.support.AbstractRecordSetLoader;
import marmot.plan.LoadOptions;
import marmot.proto.optor.LoadSpatialClusterFileProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;
import utils.UnitUtils;
import utils.func.FOption;
import utils.stream.FStream;


/**
 * {@code LoadMarmotFile}연산은 지정된 HDFS 파일 (또는 복수개의 HDFS 파일들)을 읽어 저장된
 * 레코드를 읽어 레코드 세트로 생성하는 작업을 수행한다.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LoadSpatialClusterFile extends AbstractRecordSetLoader
									implements MapReduceableRecordSetLoader,
												PBSerializable<LoadSpatialClusterFileProto> {
	static final Logger s_logger = LoggerFactory.getLogger(LoadSpatialClusterFile.class);
	
	private final Path m_path;
	private FOption<Envelope> m_range = FOption.empty();
	private LoadOptions m_options = LoadOptions.DEFAULT;
	
	@Nullable private SpatialClusterFile m_scFile;	// set when initialized
	@Nullable private FOption<Envelope> m_range84;	// set when initialized
	
	public LoadSpatialClusterFile(Path path) {
		m_path = path;
		
		setLogger(s_logger);
	}
	
	public LoadSpatialClusterFile setOptions(LoadOptions options) {
		m_options = options;
		return this;
	}
	
	public LoadSpatialClusterFile setQueryRange(Envelope range) {
		m_range = FOption.ofNullable(range);
		return this;
	}

	@Override
	public void initialize(MarmotCore marmot) {
		HdfsPath path = HdfsPath.of(marmot.getHadoopConfiguration(), m_path);
		m_scFile = SpatialClusterFile.of(path);
		
		String srid = m_scFile.getGRecordSchema().getSrid();
		m_range84 = m_range.map(r -> CoordinateTransform.transformToWgs84(r, srid));
		
		setInitialized(marmot, m_scFile.getGRecordSchema().getRecordSchema());
	}

	@Override
	public String getInputString() {
		return m_path.toString();
	}

	@Override
	public void configure(Job job) {
		// 입력 파일 포맷 설정
		if ( m_options.mapperCount().getOrElse(-1) >= 0 ) {
			job.setInputFormatClass(FixedMapperFileInputFormat.class);
		}
		else {
			job.setInputFormatClass(SpatialClusterInputFileFormat.class);
		}

		// MapReduce 입력 파일 경로 설정 (clustered file의 인덱스 파일의 경로명)
		try {
			FileInputFormat.addInputPath(job, m_path);
		}
		catch ( IOException e ) {
			throw new RecordSetException(e);
		}
		
		// 기타 설정
		Configuration conf = job.getConfiguration();
		Parameters params = new Parameters(m_scFile.getPath().getPath(), m_scFile.getGRecordSchema(),
											m_range.getOrNull(), m_range84.getOrNull());
		SpatialClusterInputFileFormat.setParameters(conf, params);
		
		int mapperCount = m_options.mapperCount().getOrElse(-1);
		if ( mapperCount >= 0 ) {
			int vCoreCount = m_marmot.getClusterMetric().getTotalVCoreCount();
			if ( mapperCount == 0 ) {
				mapperCount = Math.round(vCoreCount * 0.9f);
			}
			else {
				mapperCount = Math.min(mapperCount, vCoreCount);
			}

			String jobId = UUID.randomUUID().toString();
			FixedMapperFileInputFormat.Parameters fparams
					= new FixedMapperFileInputFormat.Parameters(jobId, mapperCount,
													SpatialClusterInputFileFormat.class.getName());
			FixedMapperFileInputFormat.setParameters(conf, fparams);
		}
	}

	@Override
	public PlanMRExecutionMode getExecutionMode(MarmotCore marmot) throws IOException {
		FileSystem fs = marmot.getHadoopFileSystem();

		// 인덱스 파일은 압축되어있기 때문에, 0.5배 정도 크기가 큰것으로 가정함
		long totalBytes = calcTotalBytes(marmot);
		long blockSize = MapReduceableRecordSetLoader.getBlockSize(fs, Lists.newArrayList(m_path));
		int nblocks = (int)Math.ceil(totalBytes / (double)blockSize);
		
		PlanMRExecutionMode mode;
		if ( nblocks > marmot.getlocallyExecutableBlockLimitHard() ) {
			mode = PlanMRExecutionMode.NON_LOCALLY_EXECUTABLE;
		}
		else if ( nblocks <= marmot.getLocallyExecutableBlockLimitSoft() ) {
			mode = PlanMRExecutionMode.LOCALLY_EXECUTABLE;
		}
		else {
			mode = PlanMRExecutionMode.CONDITIONALLY_EXECUTABLE;
		}
		
		if ( s_logger.isInfoEnabled() ) {
			String details
				= m_range84.map(r -> m_scFile.queryClusterInfos(r)
											.map(info -> String.format("%s(%s)", info.quadKey(),
															UnitUtils.toByteSizeString(info.length())))
											.join(','))
						.getOrElse("all");
			s_logger.info("ExecutionMode estimation={}: nbytes={}, nblocks={}, clusters={}",
							mode, UnitUtils.toByteSizeString(totalBytes), nblocks, details);
		}
		return mode;
	}

	@Override
	public RecordSet load() {
		checkInitialized();
		
		FStream<Record> records;
		if ( m_range.isPresent() ) {
			records = m_scFile.queryRecord(m_range84.get(), m_range.get());
		}
		else {
			records = m_scFile.read();
		}
		
		return RecordSet.from(getRecordSchema(), records);
	}
	
	@Override
	public String toString() {
		String rangeStr = (m_range84 != null) ?  ", range=" + m_range84 : "";
		return String.format("%s: path=%s%s", getClass().getSimpleName(), m_path, rangeStr);
	}

	public static LoadSpatialClusterFile fromProto(LoadSpatialClusterFileProto proto) {
		LoadOptions opts = LoadOptions.fromProto(proto.getOptions());
		LoadSpatialClusterFile load = new LoadSpatialClusterFile(new Path(proto.getPath()))
											.setOptions(opts);
		switch ( proto.getOptionalRangeCase() ) {
			case RANGE:
				load.setQueryRange(PBUtils.fromProto(proto.getRange()));
				break;
			default:
		}
		
		return load;
	}

	@Override
	public LoadSpatialClusterFileProto toProto() {
		LoadSpatialClusterFileProto.Builder builder
								= LoadSpatialClusterFileProto.newBuilder()
															.setPath(m_scFile.getPath().toString())
															.setOptions(m_options.toProto());
		builder = m_range.transform(builder, (b,r) -> b.setRange(PBUtils.toProto(r)));
		return builder.build();
	}
	
	private long calcTotalBytes(MarmotCore marmot) {
		List<String> qkList = m_range84.map(range84 -> m_scFile.queryClusterKeys(range84))
										.getOrElse(() -> FStream.from(m_scFile.getClusterKeyAll()))
										.toList();
		
		long nbytes = FStream.from(qkList)
						.map(m_scFile::getClusterInfo)
						.mapToLong(SpatialClusterInfo::length)
						.fold(0L, (a,l) -> a+Math.round(l*1.1));
		
		return Math.round(nbytes + (qkList.size()-1) * (nbytes * 0.1));
	}
}
