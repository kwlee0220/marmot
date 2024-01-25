package marmot.optor;


import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import marmot.MarmotCore;
import marmot.MarmotCoreBuilder;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.TestUtils;
import utils.CIString;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestProject {
	private static MarmotCore s_marmot;
	
	@BeforeClass
	public static void setupForClass() throws Exception {
		s_marmot = new MarmotCoreBuilder().forLocalMR().build();
	}
	
	@Test
	public void test0() throws Exception {
		Plan plan = Plan.builder("test")
							.load("교통/지하철/서울역사")
							.project("sub_sta_sn,kor_sub_nm")
							.build();
		
		_test0(plan);
		_test0(TestUtils.toProtoAndFromIt(plan));
	}
	
	private void _test0(Plan plan) {	
		RecordSet rset = s_marmot.executeLocally(plan);
		
		long count = rset.count();
		Assert.assertEquals(286, count);
		
		RecordSchema schema = rset.getRecordSchema();
		Assert.assertEquals(2, schema.getColumnCount());
		Assert.assertEquals(CIString.of("sub_sta_sn"), CIString.of(schema.getColumnAt(0).name()));
		Assert.assertEquals(CIString.of("kor_sub_nm"), CIString.of(schema.getColumnAt(1).name()));
	}
}
