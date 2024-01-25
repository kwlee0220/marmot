package marmot.optor.geo.index;

import static utils.Utilities.checkNotNullArgument;

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

import utils.UnitUtils;
import utils.func.FOption;
import utils.stream.FStream;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.dataset.DataSetImpl;
import marmot.geo.CoordinateTransform;
import marmot.io.geo.index.SpatialIndexedCluster;
import marmot.io.geo.index.SpatialIndexedFile;
import marmot.io.mapreduce.spindex.SpatialIndexedFileInputFormat;
import marmot.io.mapreduce.spindex.SpatialIndexedFileInputFormat.Parameters;
import marmot.mapreduce.PlanMRExecutionMode;
import marmot.mapreduce.input.fixed.FixedMapperFileInputFormat;
import marmot.optor.MapReduceableRecordSetLoader;
import marmot.optor.geo.SpatialRelation;
import marmot.optor.geo.filter.FilterSpatially;
import marmot.optor.support.AbstractRecordSetLoader;
import marmot.plan.LoadOptions;
import marmot.plan.PredicateOptions;
import marmot.proto.optor.LoadSpatialClusteredFileProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;


/**
 * {@code LoadSpatialClusteredFile}연산은 지정된 HDFS 파일 (또는 복수개의 HDFS 파일들)을 읽어 저장된
 * 레코드를 읽어 레코드 세트로 생성하는 작업을 수행한다.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LoadSpatialIndexedFile extends AbstractRecordSetLoader
										implements MapReduceableRecordSetLoader,
												PBSerializable<LoadSpatialClusteredFileProto> {
	static final Logger s_logger = LoggerFactory.getLogger(LoadSpatialIndexedFile.class);
	
	private final String m_dsId;
	private FOption<Envelope> m_range = FOption.empty();
	private LoadOptions m_options = LoadOptions.DEFAULT;
	
	@Nullable private DataSetImpl m_ds;					// initialize() 호출시 설정됨
	@Nullable private SpatialIndexedFile m_idxFile;	// initialize() 호출시 설정됨
	@Nullable FOption<Envelope> m_range84 = null;
	
	public LoadSpatialIndexedFile(String dsId) {
		checkNotNullArgument(dsId, "dataset id is null");
		
		m_dsId = dsId;
	}
	
	public LoadSpatialIndexedFile setQueryRange(Envelope range) {
		m_range = FOption.ofNullable(range);
		return this;
	}
	
	public LoadSpatialIndexedFile setOptions(LoadOptions options) {
		m_options = options;
		return this;
	}
	
	@Override
	public void initialize(MarmotCore marmot) {
		m_ds = marmot.getDataSet(m_dsId);
		m_idxFile = (SpatialIndexedFile)m_ds.getSpatialIndexFile();
		
		m_range84 = m_range.map(r -> CoordinateTransform.transformToWgs84(r, m_ds.getSrid()));
		
		setInitialized(marmot, m_ds.getRecordSchema());
	}

	@Override
	public String getInputString() {
		return m_marmot.getCatalog()
						.generateSpatialIndexPath(m_dsId, m_ds.getGeometryColumn()).toString();
	}

	@Override
	public void configure(Job job) {
		// 입력 파일 포맷 설정
		if ( m_options.mapperCount().getOrElse(-1) >= 0 ) {
			job.setInputFormatClass(FixedMapperFileInputFormat.class);
		}
		else {
			job.setInputFormatClass(SpatialIndexedFileInputFormat.class);
		}

		// MapReduce 입력 파일 경로 설정 (clustered file의 인덱스 파일의 경로명)
		try {
			Path path = m_idxFile.getClusterDir().getPath();
			FileInputFormat.addInputPath(job, path);
		}
		catch ( IOException e ) {
			throw new RecordSetException(e);
		}
		
		// 기타 설정
		Configuration conf = job.getConfiguration();
		
		Envelope range84 = m_range.map(range -> CoordinateTransform.transformToWgs84(range, m_ds.getSrid()))
									.getOrNull();
		Parameters params = new Parameters(m_ds.getGeometryColumnInfo(), m_range.getOrNull(), range84);
		SpatialIndexedFileInputFormat.setParameters(conf, params);
		
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
													SpatialIndexedFileInputFormat.class.getName());
			FixedMapperFileInputFormat.setParameters(conf, fparams);
		}
	}

	@Override
	public PlanMRExecutionMode getExecutionMode(MarmotCore marmot) throws IOException {
		FileSystem fs = marmot.getHadoopFileSystem();

		// 인덱스 파일은 압축되어있기 때문에, 0.5배 정도 크기가 큰것으로 가정함
		long totalBytes = Math.round(calcTotalBytes(marmot) * 1.5);
		Path path = m_idxFile.getClusterDir().getPath();
		long blockSize = MapReduceableRecordSetLoader.getBlockSize(fs, Lists.newArrayList(path));
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
				= m_range84.map(r -> m_idxFile.queryClusters(r)
											.map(c -> String.format("%s(%s)", c.getQuadKey(),
															UnitUtils.toByteSizeString(c.length())))
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

		FStream<Record> records = m_range84.map(m_idxFile::query)
											.getOrElse(m_idxFile::read);
		RecordSet loaded = RecordSet.from(getRecordSchema(), records);
		if ( m_range.isPresent() ) {
			FilterSpatially filter = new FilterSpatially(m_ds.getGeometryColumn(),
														SpatialRelation.INTERSECTS, m_range.get(),
														PredicateOptions.DEFAULT);
			filter.initialize(m_marmot, m_ds.getRecordSchema());
			loaded = filter.apply(loaded);
		}
		
		return loaded;
	}
	
	@Override
	public String toString() {
		return String.format("%s: ds=%s", getClass().getSimpleName(), m_dsId);
	}

	public static LoadSpatialIndexedFile fromProto(LoadSpatialClusteredFileProto proto) {
		return new LoadSpatialIndexedFile(proto.getDataset())
						.setQueryRange(PBUtils.fromProto(proto.getRange()));
	}

	@Override
	public LoadSpatialClusteredFileProto toProto() {
		LoadSpatialClusteredFileProto.Builder builder = LoadSpatialClusteredFileProto.newBuilder()
																				.setDataset(m_dsId);
		return m_range.transform(builder, (b,r) -> b.setRange(PBUtils.toProto(r))).build();
	}
	
	private long calcTotalBytes(MarmotCore marmot) {
		List<String> qkList = m_range84.map(range84 -> m_idxFile.queryClusterKeys(range84))
										.getOrElse(() -> FStream.from(m_idxFile.getClusterKeyAll()))
										.toList();
		
		long nbytes = FStream.from(qkList)
						.map(m_idxFile::getCluster)
						.mapToInt(SpatialIndexedCluster::length)
						.fold(0L, (a,l) -> a+Math.round(l*1.1));
		
		return Math.round(nbytes + (qkList.size()-1) * (nbytes * 0.1));
	}
}
