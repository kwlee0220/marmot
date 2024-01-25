package marmot.optor;


import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import marmot.MarmotCore;
import marmot.MarmotCoreBuilder;
import marmot.Plan;
import marmot.Record;
import marmot.RecordScript;
import marmot.RecordSet;
import marmot.TestUtils;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestUpdate {
	private static MarmotCore s_marmot;
	
	@BeforeClass
	public static void setupForClass() throws Exception {
		s_marmot = new MarmotCoreBuilder().forLocalMR().build();
	}
	
	@Test
	public void test0() throws Exception {
		Plan plan = Plan.builder("test")
							.loadMarmotFile("database/heap/교통/지하철/서울역사")
							.update("kor_SUB_nm += '|' + sub_sta_SN")
							.build();
		
		_test0(plan);
		_test0(TestUtils.toProtoAndFromIt(plan));
	}
	
	private void _test0(Plan plan) {	
		RecordSet rset = s_marmot.executeLocally(plan);
		
		long count = rset.count();
		Assert.assertEquals(286, count);
		
		rset = s_marmot.executeLocally(plan);
		for ( Record record: rset.toList() ) {
			String name = record.getString("KOR_sub_nm");
			int sn = record.getInt("Sub_Sta_Sn");
			String[] parts = name.split("\\|");
			
			Assert.assertEquals(2, parts.length);
			Assert.assertEquals(sn, Integer.parseInt(parts[1]));
		}
	}
	
	@Test
	public void test1() throws Exception {
		RecordScript setStaSn = RecordScript.of("suB_sta_sn = param")
												.addArgument("param", 999);
		
		Plan plan = Plan.builder("test")
							.load("교통/지하철/서울역사")
							.update(setStaSn)
							.build();
		
		_test1(plan);
		_test1(TestUtils.toProtoAndFromIt(plan));
	}

	private void _test1(Plan plan) {	
		RecordSet rset = s_marmot.executeLocally(plan);
		
		long count = rset.count();
		Assert.assertEquals(286, count);
		
		rset = s_marmot.executeLocally(plan);
		for ( Record record: rset.toList() ) {
			int sn = record.getInt("sub_sta_sn");
			Assert.assertEquals(sn, 999);
		}
	}

	@Test
	public void test2() throws Exception {
		RecordScript setStaSn = RecordScript.of("$init = 999", "sub_sta_sn = $init");
		
		Plan plan = Plan.builder("test")
							.loadMarmotFile("database/heap/교통/지하철/서울역사")
							.update(setStaSn)
							.build();
		
		_test1(plan);
		_test1(TestUtils.toProtoAndFromIt(plan));
	}
}
