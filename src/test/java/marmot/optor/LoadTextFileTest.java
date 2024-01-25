package marmot.optor;


import java.io.PrintWriter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import marmot.MarmotCore;
import marmot.MarmotCoreBuilder;
import marmot.Plan;
import marmot.RecordSet;
import marmot.TestUtils;
import marmot.io.HdfsPath;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LoadTextFileTest {
	private static MarmotCore s_marmot;
	private static FileSystem s_fs;
	
	@BeforeClass
	public static void setupForClass() throws Exception {
		s_marmot = new MarmotCoreBuilder().forLocalMR().build();
		s_fs = s_marmot.getHadoopFileSystem();
	}
	
//	@Test
	public void test0() throws Exception {
		Plan plan = Plan.builder("test")
							.loadTextFile("data/로그/dtg_t/20160901_S.csv")
							.build();
		
		_test0(plan);
		_test0(TestUtils.toProtoAndFromIt(plan));
	}
	
	private void _test0(Plan plan) {	
		RecordSet rset = s_marmot.executeLocally(plan);
		
		long count = rset.count();
		Assert.assertEquals(3000000, count);
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

		Plan plan;
		plan = Plan.builder("test")
							.loadTextFile("tmp/test")
							.build();

		try ( RecordSet rset = s_marmot.executeLocally(plan) ) {
			Assert.assertEquals(6, rset.count());
		}
		
		HdfsPath.of(s_fs, new Path("tmp/test")).delete();
	}
}
