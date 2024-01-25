package marmot.optor;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import marmot.MarmotCore;
import marmot.MarmotCoreBuilder;
import marmot.Plan;
import marmot.TestUtils;
import marmot.io.HdfsPath;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestStoreAsCsv {
	private static MarmotCore s_marmot;
	private static Configuration s_conf;
	
	@BeforeClass
	public static void setupForClass() throws Exception {
		s_marmot = new MarmotCoreBuilder().forLocalMR().build();
		s_marmot.getFileServer().deleteFile(new Path("tmp/result"));
		
		s_conf = s_marmot.getHadoopConfiguration();
	}
	
	@Test
	public void test0() throws Exception {
		Plan plan = Plan.builder("test")
							.loadMarmotFile("database/heap/교통/지하철/서울역사")
							.filter("sub_sta_sn > 320")
							.project("kor_sub_nm,sub_sta_sn,sig_cd")
							.storeAsCsv("tmp/result", StoreAsCsvOptions.DEFAULT('@'))
							.build();
		
		_test0(plan);
		s_marmot.getFileServer().deleteFile(new Path("tmp/result"));
		
		_test0(TestUtils.toProtoAndFromIt(plan));
		s_marmot.getFileServer().deleteFile(new Path("tmp/result"));
	}
	
	private void _test0(Plan plan) throws IllegalArgumentException, IOException {
		HdfsPath tmp = HdfsPath.of(s_conf, new Path("tmp/result"));
		Assert.assertTrue(!tmp.exists());
				
		s_marmot.execute(plan);
		Assert.assertTrue(tmp.exists());
		
		FileSystem fs = s_marmot.getHadoopFileSystem();
		try ( LineReader reader = new LineReader(fs.open(new Path("tmp/result"))) ) {
			Text text = new Text();
			while ( reader.readLine(text) > 0 ) {
				String[] parts = text.toString().split("@");
				Assert.assertEquals(3, parts.length);
				Assert.assertTrue(Integer.parseInt(parts[1]) > 320);
			}
		}
	}
}
