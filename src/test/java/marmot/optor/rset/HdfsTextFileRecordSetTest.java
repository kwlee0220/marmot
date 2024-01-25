package marmot.optor.rset;

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
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
import marmot.RecordSet;
import marmot.io.HdfsPath;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@RunWith(MockitoJUnitRunner.class)
public class HdfsTextFileRecordSetTest {
	private static MarmotCore s_marmot;
	private static FileSystem s_fs;
	
	@BeforeClass
	public static void setupForClass() throws Exception {
		s_marmot = new MarmotCoreBuilder().forLocalMR().build();
		s_fs = s_marmot.getHadoopFileSystem();
	}
	
	@Before
	public void setup() {
	}
	
	@Test
	public void test0() throws Exception {
		try ( RecordSet rset = HdfsTextFileRecordSet.of() ) {
			Assert.assertEquals(0, rset.count());
		}
	}
	
	@Test
	public void test1() throws Exception {
		HdfsPath path = HdfsPath.of(s_fs, new Path("tmp/test"));
		try ( PrintWriter writer = new PrintWriter(path.create()) ) {
			writer.println("1");
			writer.println("2");
			writer.println("3");
		}
		
		try ( RecordSet rset = HdfsTextFileRecordSet.of(path) ) {
			for ( int i = 0; i < 3; ++i ) {
				Record rec = rset.nextCopy();
				int v = Integer.parseInt(rec.getString(0));
				Assert.assertEquals(i+1, v);
			}
			Assert.assertEquals(null, rset.nextCopy());
		}
		path.delete();
	}
	
	@Test
	public void test2() throws Exception {
		HdfsPath path1 = HdfsPath.of(s_fs, new Path("tmp/test/file1"));
		try ( PrintWriter writer = new PrintWriter(path1.create()) ) {
			writer.println("1");
			writer.println("2");
			writer.println("3");
		}
		HdfsPath path2 = HdfsPath.of(s_fs, new Path("tmp/test/file2"));
		try ( PrintWriter writer = new PrintWriter(path2.create()) ) {
			writer.println("4");
			writer.println("5");
		}
		HdfsPath path3 = HdfsPath.of(s_fs, new Path("tmp/test/file3"));
		try ( PrintWriter writer = new PrintWriter(path3.create()) ) {
			writer.println("6");
		}
		HdfsPath path4 = HdfsPath.of(s_fs, new Path("tmp/test/file4"));
		try ( PrintWriter writer = new PrintWriter(path4.create()) ) {
		}
		
		List<HdfsPath> starts = Arrays.asList(path1, path2, path3, path4);

		try ( RecordSet rset = HdfsTextFileRecordSet.of(starts) ) {
			Assert.assertEquals(6, rset.count());
		}
		
		int[] tags = new int[6];
		try ( RecordSet rset = HdfsTextFileRecordSet.of(starts) ) {
			for ( int i = 0; i < tags.length; ++i ) {
				Record rec;
				rec = rset.nextCopy();
				int v = Integer.parseInt(rec.getString(0));
				Assert.assertEquals(i+1, v);
			}
			Assert.assertEquals(null, rset.nextCopy());
		}
		
		HdfsPath.of(s_fs, new Path("tmp/test")).delete();
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void test3() throws Exception {
		try ( RecordSet rset = HdfsTextFileRecordSet.of((HdfsPath)null) ) {
			Assert.assertEquals(0, rset.count());
		}
	}
}
