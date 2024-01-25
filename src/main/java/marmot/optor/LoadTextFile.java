package marmot.optor;

import java.util.Arrays;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.LoggerFactory;

import marmot.MarmotCore;
import marmot.RecordSet;
import marmot.io.mapreduce.textfile.MarmotTextInputFormat;
import marmot.mapreduce.input.fixed.FixedMapperFileInputFormat;
import marmot.mapreduce.support.MRHeapFileRecordSetLoader;
import marmot.optor.rset.HdfsTextFileRecordSet;
import marmot.plan.LoadOptions;
import marmot.proto.optor.LoadTextFileProto;
import marmot.support.PBSerializable;
import utils.stream.FStream;


/**
 * {@code LoadTextFile}연산은 지정된 HDFS 테스트 파일을 읽어 저장된 각 테스트 라인을
 * 하나의 레코드로 하는 레코드 세트를 구성하는 작업을 수행한다.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LoadTextFile extends MRHeapFileRecordSetLoader
							implements PBSerializable<LoadTextFileProto> {
	public static LoadTextFile from(String... pathes) {
		return new LoadTextFile(Arrays.asList(pathes), LoadOptions.DEFAULT);
	}
	
	private LoadTextFile(Iterable<String> pathList, LoadOptions opts) {
		super(pathList, opts);
		
		setLogger(LoggerFactory.getLogger(LoadTextFile.class));
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Class<? extends InputFormat> getInputFormatClass() {
		if ( m_options.mapperCount().getOrElse(-1) >= 0 ) {
			return FixedMapperFileInputFormat.class;
		}
		else {
			return MarmotTextInputFormat.class;
		}
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
															MarmotTextInputFormat.class.getName());
			FixedMapperFileInputFormat.setParameters(conf, params);
		}
	}
	
	@Override
	public void initialize(MarmotCore marmot) {
		setInitialized(marmot, HdfsTextFileRecordSet.SCHEMA);
	}

	@Override
	public RecordSet load() {
		checkInitialized();
		
		return HdfsTextFileRecordSet.of(m_marmot.getHadoopFileSystem(), getPathList());
	}
	
	@Override
	public String toString() {
		return String.format("load_textfile[pathes=%s,nsplits=%d]",
							getPathList(), getSplitCountPerBlock());
	}

	public static LoadTextFile fromProto(LoadTextFileProto proto) {
		LoadOptions opts = LoadOptions.fromProto(proto.getOptions());
		return new LoadTextFile(proto.getPathsList(), opts);
	}
	
	public LoadTextFileProto toProto() {
		return LoadTextFileProto.newBuilder()
								.addAllPaths(FStream.from(m_pathList)
										.map(Path::toString)
										.toList())
								.setOptions(m_options.toProto())
								.build();
	}
}
