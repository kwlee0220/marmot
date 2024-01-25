package marmot.geowave;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.LoggerFactory;

import com.forcewave.ghdfs.obj.GMetaObject;
import com.forcewave.ghdfs.obj.GObject;
import com.forcewave.ghdfs.reader.GHDFSReader;
import com.google.common.collect.Lists;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSetException;
import marmot.optor.RecordSetOperatorException;
import marmot.optor.support.RecordLevelTransform;
import marmot.proto.optor.ParseGObjectProto;
import marmot.support.PBSerializable;
import marmot.type.DataType;
import utils.func.Tuple;
import utils.stream.FStream;


/**
 * {@code LoadTextFile}연산은 지정된 HDFS 테스트 파일을 읽어 저장된 각 테스트 라인을
 * 하나의 레코드로 하는 레코드 세트를 구성하는 작업을 수행한다.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ParseGObject extends RecordLevelTransform
							implements PBSerializable<ParseGObjectProto> {
	private final String m_layerName;
	private RecordSchema m_schema;
	private int m_textColIdx = -1;
	private GMetaObject m_gmeta;
	
	public ParseGObject(String layerName) {
		m_layerName = layerName;
		setLogger(LoggerFactory.getLogger(ParseGObject.class));
	}
	
	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		FileSystem fs = marmot.getHadoopFileSystem();
		GHDFSReader reader = new GHDFSReader(fs, m_layerName);
		try {
			reader.open();
			m_gmeta = reader.getMeta();
			
			m_textColIdx = inputSchema.getColumn("text").ordinal();
			m_schema = FStream.from(m_gmeta.getColumnInfo())
							.zipWithIndex()
							.map(tup -> fromGhdfsColumnInfo(tup._2, tup._1))
							.fold(RecordSchema.builder(), (b,t) -> b.addColumn(t._1, t._2))
							.build();
			
			super.setInitialized(marmot, inputSchema, m_schema);
		}
		catch ( Exception e ) {
 			throw new RecordSetOperatorException(String.format("op=%s, cause=%s", this, e));
		}
		finally {
			reader.close();
		}
	}

	@Override
	public boolean transform(Record input, Record output) {
		try {
			String encoded = input.getString(m_textColIdx);
			GObject gobj = GObject.deserialize(encoded, m_gmeta);

			for ( int i =0; i < gobj.getColumnCount(); ++i ) {
				output.set(i, gobj.getColumnItem(i));
			}
			
			return true;
		}
		catch ( Exception e ) {
			throw new RecordSetException(e);
		}
	}
	
	@Override
	public String toString() {
		return String.format("parse_gobject");
	}

	public static ParseGObject fromProto(ParseGObjectProto proto) {
		return new ParseGObject(proto.getLayerName());
	}
	
	public ParseGObjectProto toProto() {
		return ParseGObjectProto.newBuilder()
								.setLayerName(m_layerName)
								.build();
	}
	
	private Tuple<String,DataType> fromGhdfsColumnInfo(int index, Pair<String,String> info) {
		if ( index == m_gmeta.getSpatialColumnIdx() ) {
			return Tuple.of(info.getKey(),
							GHdfsUtils.fromGhdfsSpatialType(m_gmeta.getSpatialType()));
		}
		else {
			return Tuple.of(info.getKey(), GHdfsUtils.fromGhdfsColumnType(info.getValue()));
		}
	}
	
	private static List<String> toInputPathString(String layerName) {
		return Lists.newArrayList(GHdfsUtils.toPath(layerName));
	}
}
