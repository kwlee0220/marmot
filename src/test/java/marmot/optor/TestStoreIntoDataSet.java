package marmot.optor;


import org.apache.hadoop.fs.FileSystem;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import marmot.MarmotCore;
import marmot.MarmotCoreBuilder;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSet;
import marmot.TestUtils;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestStoreIntoDataSet {
	private static MarmotCore s_marmot;
	private static FileSystem s_fs;
	
	@BeforeClass
	public static void setupForClass() throws Exception {
		s_marmot = new MarmotCoreBuilder().forLocalMR().build();
		s_marmot.deleteDataSet("tmp/result");
		
		s_fs = s_marmot.getHadoopFileSystem();
	}
	
	@Test
	public void test0() throws Exception {
		Plan plan = Plan.builder("test")
							.load("교통/지하철/서울역사")
							.filter("sub_sta_sn > 320")
							.store("tmp/result")
							.build();
		
		_test0(plan);
		s_marmot.deleteDataSet("tmp/result");
		
		_test0(TestUtils.toProtoAndFromIt(plan));
		s_marmot.deleteDataSet("tmp/result");
	}
	
	private void _test0(Plan plan) {
		Assert.assertEquals(null, s_marmot.getDataSetOrNull("tmp/result"));
		s_marmot.createDataSet("tmp/result", s_marmot.getOutputRecordSchema(plan),
								CreateDataSetOptions.FORCE);
		s_marmot.execute(plan);
		Assert.assertEquals(6, s_marmot.getDataSetOrNull("tmp/result").getRecordCount());
		
		RecordSet rset = s_marmot.getDataSetOrNull("tmp/result").read();
		for ( Record record: rset.toList() ) {
			Assert.assertTrue(record.getInt("sub_sta_sn") > 320);
		}
	}
}
