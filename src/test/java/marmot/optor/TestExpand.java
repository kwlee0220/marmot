package marmot.optor;


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
public class TestExpand {
	private static MarmotCore s_marmot;
	
	@BeforeClass
	public static void setupForClass() throws Exception {
		s_marmot = new MarmotCoreBuilder().forLocalMR().build();
	}
	
	@Test
	public void test0() throws Exception {
		Plan plan = Plan.builder("test")
							.loadMarmotFile("database/heap/교통/지하철/서울역사")
							.expand("expanded:string", "expanded = kor_sub_nm + '|' + sub_sta_sn")
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
			String name = record.getString("expanded");
			int sn = record.getInt("sub_sta_sn");
			String[] parts = name.split("\\|");
			
			Assert.assertEquals(2, parts.length);
			Assert.assertEquals(sn, Integer.parseInt(parts[1]));
		}
	}
}
