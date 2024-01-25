package marmot.io;


import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import marmot.MarmotCore;
import marmot.MarmotCoreBuilder;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.io.MarmotSequenceFile.Writer;
import marmot.support.DefaultRecord;
import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@RunWith(MockitoJUnitRunner.class)
public class MarmotFileTest {
	private static Configuration s_conf;
	private MarmotSequenceFile m_sido;
	private MarmotSequenceFile m_tmp;
	
	@BeforeClass
	public static void setupClass() throws Exception {
		MarmotCore marmot = new MarmotCoreBuilder().forLocalMR().build();
		s_conf = marmot.getHadoopConfiguration();
	}
	
	@Before
	public void setup() {
		m_sido = MarmotSequenceFile.of(HdfsPath.of(s_conf, new Path("database/heap/구역/시도")));
		m_tmp = MarmotSequenceFile.of(HdfsPath.of(s_conf, new Path("tmp/result")));
	}
	
	@Test
	public void test01() throws Exception {
		HdfsPath path = HdfsPath.of(s_conf, new Path("tmp/result"));
		
		RecordSchema schema = RecordSchema.builder()
											.addColumn("name", DataType.STRING)
											.addColumn("age", DataType.INT)
											.build();
		try ( Writer writer = MarmotSequenceFile.create(path, schema, null) ) {
			DefaultRecord rec = DefaultRecord.of(schema);
			rec.setAll("1", 1);
			writer.write(rec);
			rec.setAll("2", 2);
			writer.write(rec);
			rec.setAll("3", 3);
			writer.write(rec);
		}
		
		MarmotSequenceFile file = MarmotSequenceFile.of(path);
		Assert.assertEquals(schema, file.getRecordSchema());
		Assert.assertEquals(3, file.read().count());
		
		try ( RecordSet rset = file.read() ) {
			Record rec;
			
			rec = rset.nextCopy();
			Assert.assertEquals("1", rec.get(0));
			Assert.assertEquals(1, rec.getInt(1));
			
			rec = rset.nextCopy();
			Assert.assertEquals("2", rec.get(0));
			Assert.assertEquals(2, rec.getInt(1));
		}
	}
	
	@Test
	public void test02() throws Exception {
		MarmotSequenceFile idx = MarmotSequenceFile.of(HdfsPath.of(s_conf, new Path("database/spatial_indexes/구역/시군구/the_geom/cluster.idx")));
		
		Map<String,String> metadata = idx.getFileInfo().getMetadata();
		String schemaStr = metadata.get(MarmotSequenceFile.MARMOT_FILE_KEY_PREFIX + "cluster_schema");
		RecordSchema schema = RecordSchema.parse(schemaStr);
		Assert.assertEquals("the_geom", schema.getColumnAt(0).name());
		
	}
	
	@Test(expected=MarmotFileNotFoundException.class)
	public void test03() throws Exception {
		MarmotSequenceFile file = MarmotSequenceFile.of(HdfsPath.of(s_conf, new Path("databasex")));
		try ( RecordSet rset = file.read() ) {
		}
		
	}
}
