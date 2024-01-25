package marmot.optor;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import marmot.MarmotCore;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.RecordSetException;
import marmot.io.HdfsPath;
import marmot.io.MarmotFileException;
import marmot.plan.LoadOptions;
import marmot.plan.STScriptPlanLoader;
import marmot.proto.optor.LoadCustomTextFileProto;
import marmot.proto.optor.LoadTextFileProto;
import marmot.support.PBSerializable;
import marmot.support.RecordSetOperatorChain;


/**
 * {@code LoadCustomTextFile}연산은 지정된 HDFS 테스트 파일을 읽어 저장된 각 테스트 라인을
 * 하나의 레코드로 하는 레코드 세트를 구성하는 작업을 수행한다.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LoadCustomTextFile extends CompositeRecordSetLoader
								implements PBSerializable<LoadCustomTextFileProto> {
	private static final String ST_PLAN_SUFFIX = "meta.st";
	private static final String JSON_PLAN_SUFFIX = "meta.json";
	
	private final String m_path;
	private final LoadOptions m_options;
	private RecordSetOperatorChain m_loadChain;
	
	public LoadCustomTextFile(String pathStr) {
		this(pathStr, LoadOptions.DEFAULT);
	}
	
	public LoadCustomTextFile(String pathStr, LoadOptions opts) {
		m_path = pathStr;
		m_options = opts;
		
		setLogger(LoggerFactory.getLogger(LoadCustomTextFile.class));
	}

	@Override
	protected RecordSchema _initialize(MarmotCore marmot) {
		m_loadChain = createChain(marmot);
		return m_loadChain.getOutputRecordSchema();
	}

	@Override
	protected RecordSetOperatorChain createComponents() {
		Preconditions.checkState(m_loadChain != null);
		
		return m_loadChain;
	}

	public static LoadCustomTextFile fromProto(LoadCustomTextFileProto proto) {
		LoadOptions opts = LoadOptions.fromProto(proto.getOptions());
		return new LoadCustomTextFile(proto.getPath(), opts);
	}

	@Override
	public LoadCustomTextFileProto toProto() {
		return LoadCustomTextFileProto.newBuilder()
								.setPath(m_path.toString())
								.setOptions(m_options.toProto())
								.build();
	}
	
	@Override
	public String toString() {
		return String.format("load_custom_textfile[path=%s]", m_path);
	}

	private RecordSetOperatorChain createChain(MarmotCore marmot) {
		try {
			Plan plan = null;
			
			HdfsPath path = HdfsPath.of(marmot.getHadoopFileSystem(), new Path(m_path));
			HdfsPath metaFile = getMetaPlanFile(path, ST_PLAN_SUFFIX);
			if ( metaFile.exists() ) {
				try ( InputStream is = metaFile.open() ) {
					plan = STScriptPlanLoader.load(is, StandardCharsets.UTF_8);
				}
			}
			else {
				metaFile = getMetaPlanFile(path, JSON_PLAN_SUFFIX);
				if ( metaFile.exists() ) {
					try ( InputStream is = metaFile.open() ) {
						plan = STScriptPlanLoader.load(is, StandardCharsets.UTF_8);
					}
				}
			}
			if ( plan == null ) {
				throw new RecordSetException("fails to find meta-file: path=" + path);
			}
			
			RecordSetOperatorChain components = RecordSetOperatorChain.from(marmot, plan);
			LoadTextFileProto replaced = LoadTextFileProto.newBuilder()
														.addAllPaths(Arrays.asList(m_path))
														.setOptions(m_options.toProto())
														.build();
			components.add(0, LoadTextFile.fromProto(replaced));
			
			return components;
		}
		catch ( IOException e ) {
			throw new RecordSetException("fails to load meta-file from path=" + m_path
										+ ", cause=" + e);
		}
	}
	
	private HdfsPath getMetaPlanFile(HdfsPath start, String suffix) {
		try {
			if ( start.isDirectory() ) {
				return start.child("_" + suffix);
			}
			else {
				String fileName = start.getPath().toString();
				fileName = String.format("%s%s.%s", FilenameUtils.getFullPath(fileName),
										FilenameUtils.getBaseName(fileName), suffix);
				return HdfsPath.of(start.getFileSystem(), new Path(fileName));
			}
		}
		catch ( IOException e ) {
			String msg = String.format("fails to load MetaPlanFile: path=%s, suffix=%s, cause=%s",
										start, suffix, e);
			throw new MarmotFileException(msg);
		}
	}
}
