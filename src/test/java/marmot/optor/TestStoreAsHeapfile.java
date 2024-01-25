package marmot.optor;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import marmot.MarmotCore;
import marmot.MarmotCoreBuilder;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSet;
import marmot.TestUtils;
import marmot.io.HdfsPath;
import marmot.io.MarmotSequenceFile;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestStoreAsHeapfile {
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
							.storeMarmotFile("tmp/result")
							.build();
		
		_test0(plan);
		s_marmot.getFileServer().deleteFile(new Path("tmp/result"));
		
		_test0(TestUtils.toProtoAndFromIt(plan));
		s_marmot.getFileServer().deleteFile(new Path("tmp/result"));
	}
	
	private void _test0(Plan plan) throws Exception {
		HdfsPath tmp = HdfsPath.of(s_conf, new Path("tmp/result"));
		Assert.assertTrue(!tmp.exists());
		
		s_marmot.execute(plan);
		Assert.assertTrue(tmp.exists());
		
		try ( RecordSet rset = MarmotSequenceFile.of(tmp).read() ) {
			for ( Record record: rset.toList() ) {
				Assert.assertTrue(record.getInt("sub_sta_sn") > 320);
			}	
		}
	}
}
