package marmot.optor;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import marmot.MarmotCore;
import marmot.MarmotCoreBuilder;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.TestUtils;
import marmot.io.HdfsPath;
import marmot.io.MarmotSequenceFile;
import marmot.io.MarmotSequenceFile.Writer;
import marmot.support.DefaultRecord;
import marmot.type.DataType;
import utils.stream.IntFStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LoadMarmotFileTest {
	private static MarmotCore s_marmot;
	private static FileSystem s_fs;
	
	@BeforeClass
	public static void setupForClass() throws Exception {
		s_marmot = new MarmotCoreBuilder().forLocalMR().build();
		s_fs = s_marmot.getHadoopFileSystem();
	}
	
	@Test
	public void test0() throws Exception {
		Plan plan = Plan.builder("test")
							.loadMarmotFile("database/heap/교통/지하철/서울역사")
							.build();
		
		_test0(plan);
		_test0(TestUtils.toProtoAndFromIt(plan));
	}
	
	private void _test0(Plan plan) {	
		RecordSet rset = s_marmot.executeLocally(plan);
		
		long count = rset.count();
		Assert.assertEquals(286, count);
	}
	
	@Test
	public void test02() throws Exception {
		RecordSchema schema = RecordSchema.builder()
										.addColumn("name", DataType.STRING)
										.addColumn("age", DataType.INT)
										.build();
		
		HdfsPath path = HdfsPath.of(s_fs, new Path("tmp/test/file1"));
		try ( Writer writer = MarmotSequenceFile.create(path, schema, null) ) {
			DefaultRecord rec = DefaultRecord.of(schema);
			rec.setAll("1", 1);
			writer.write(rec);
			rec.setAll("2", 2);
			writer.write(rec);
			rec.setAll("3", 3);
			writer.write(rec);
		}
		path = HdfsPath.of(s_fs, new Path("tmp/test/file2"));
		try ( Writer writer = MarmotSequenceFile.create(path, schema, null) ) {
			DefaultRecord rec = DefaultRecord.of(schema);
			rec.setAll("4", 4);
			writer.write(rec);
			rec.setAll("5", 5);
			writer.write(rec);
		}
		path = HdfsPath.of(s_fs, new Path("tmp/test/file3"));
		try ( Writer writer = MarmotSequenceFile.create(path, schema, null) ) {
			DefaultRecord rec = DefaultRecord.of(schema);
			rec.setAll("6", 6);
			writer.write(rec);
			rec.setAll("7", 7);
			writer.write(rec);
		}
		
		Plan plan;
		plan = Plan.builder("test")
							.loadMarmotFile("tmp/test")
							.build();

		try ( RecordSet rset = s_marmot.executeLocally(plan) ) {
			Assert.assertEquals(7, rset.count());
		}
		
		int[] tags = new int[7];
		try ( RecordSet rset = s_marmot.executeLocally(plan) ) {
			for ( int i = 0; i < tags.length; ++i ) {
				Record rec;
				rec = rset.nextCopy();
				int v = rec.getInt(1);
				tags[v-1] = 1;
				Assert.assertEquals(""+v, rec.getString(0));
			}
			Assert.assertEquals(null, rset.nextCopy());
		}
		Assert.assertEquals(7, IntFStream.of(tags).sum());

		plan = Plan.builder("test")
							.loadMarmotFile("tmp/test/file1", "tmp/test/file2",
									"tmp/test/file3")
							.build();

		try ( RecordSet rset = s_marmot.executeLocally(plan) ) {
			for ( int i = 0; i < tags.length; ++i ) {
				Record rec;
				rec = rset.nextCopy();
				int v = rec.getInt(1);
				tags[v-1] = 1;
				Assert.assertEquals(""+v, rec.getString(0));
			}
			Assert.assertEquals(null, rset.nextCopy());
		}
		Assert.assertEquals(7, IntFStream.of(tags).sum());
		
		HdfsPath.of(s_fs, new Path("tmp/test")).delete();
	}
}
