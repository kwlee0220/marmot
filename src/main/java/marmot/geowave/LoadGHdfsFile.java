package marmot.geowave;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.LoggerFactory;

import com.forcewave.ghdfs.obj.GMetaObject;
import com.forcewave.ghdfs.reader.GHDFSReader;

import utils.func.Tuple;
import utils.stream.FStream;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.optor.CompositeRecordSetLoader;
import marmot.optor.LoadTextFile;
import marmot.optor.RecordSetOperatorException;
import marmot.proto.optor.LoadGHdfsFileProto;
import marmot.support.PBSerializable;
import marmot.support.RecordSetOperatorChain;
import marmot.type.DataType;


/**
 * {@code LoadTextFile}연산은 지정된 HDFS 테스트 파일을 읽어 저장된 각 테스트 라인을
 * 하나의 레코드로 하는 레코드 세트를 구성하는 작업을 수행한다.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LoadGHdfsFile extends CompositeRecordSetLoader
							implements PBSerializable<LoadGHdfsFileProto> {
	private final String m_layerName;
	private GMetaObject m_gmeta;
	
	public static LoadGHdfsFile from(String layerName) {
		return new LoadGHdfsFile(layerName);
	}
	
	private LoadGHdfsFile(String layerName) {
		m_layerName = layerName;
		setLogger(LoggerFactory.getLogger(LoadGHdfsFile.class));
	}
	
	public GMetaObject getGMetaObject() {
		checkInitialized();
		
		return m_gmeta;
	}

	@Override
	protected RecordSchema _initialize(MarmotCore marmot) {
		FileSystem fs = marmot.getHadoopFileSystem();
		GHDFSReader reader = new GHDFSReader(fs, m_layerName);
		try {
			reader.open();
			
			m_gmeta = reader.getMeta();
			return FStream.from(m_gmeta.getColumnInfo())
							.zipWithIndex()
							.map(tup -> fromGhdfsColumnInfo(m_gmeta, tup.index(), tup.value()))
							.fold(RecordSchema.builder(), (b,t) -> b.addColumn(t._1, t._2))
							.build();
		}
		catch ( Exception e ) {
 			throw new RecordSetOperatorException(String.format("op=%s, cause=%s", this, e));
		}
		finally {
			reader.close();
		}
	}

	@Override
	protected RecordSetOperatorChain createComponents() {
		checkInitialized();
		
		RecordSetOperatorChain chain = RecordSetOperatorChain.from(m_marmot);
		chain.add(LoadTextFile.from(GHdfsUtils.toPath(m_layerName)));
		chain.add(new ParseGObject(m_layerName));
		
		return chain;
	}
	
	@Override
	public String toString() {
		return String.format("load_ghdfs_file[layer=%s]", m_layerName);
	}

	public static LoadGHdfsFile fromProto(LoadGHdfsFileProto proto) {
		return new LoadGHdfsFile(proto.getLayerName());
	}
	
	public LoadGHdfsFileProto toProto() {
		return LoadGHdfsFileProto.newBuilder()
								.setLayerName(m_layerName)
								.build();
	}
	
	private Tuple<String,DataType> fromGhdfsColumnInfo(GMetaObject gmeta, int index,
														Pair<String,String> info) {
		if ( index == gmeta.getSpatialColumnIdx() ) {
			return Tuple.of(info.getKey(),
							GHdfsUtils.fromGhdfsSpatialType(gmeta.getSpatialType()));
		}
		else {
			return Tuple.of(info.getKey(), GHdfsUtils.fromGhdfsColumnType(info.getValue()));
		}
	}
}
