package marmot.optor;


import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import marmot.MarmotCore;
import marmot.MarmotCoreBuilder;
import marmot.Plan;
import marmot.RecordSet;
import marmot.TestUtils;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestPickTopK {
	private static MarmotCore s_marmot;
	
	@BeforeClass
	public static void setupForClass() throws Exception {
		s_marmot = new MarmotCoreBuilder().forLocalMR().build();
	}
	
	@Test
	public void test0() throws Exception {
		Plan plan = Plan.builder("test")
							.loadMarmotFile("database/heap/교통/지하철/서울역사")
							.pickTopK("sub_sta_sn:d", 5)
							.build();
		
		_test0(plan);
		_test0(TestUtils.toProtoAndFromIt(plan));
	}
	
	private void _test0(Plan plan) {	
		RecordSet rset = s_marmot.executeLocally(plan);
		
		long count = rset.count();
		Assert.assertEquals(5, count);
		
		int[] values = s_marmot.executeLocally(plan)
								.fstream()
								.mapToInt(r -> r.getInt("sub_sta_sn"))
								.toArray();
		for ( int i =1; i < values.length; ++i ) {
			Assert.assertTrue(values[i] <= values[i-1]);
		}
	}
	
	@Test
	public void test1() throws Exception {
		Plan plan = Plan.builder("test")
							.loadMarmotFile("database/heap/교통/지하철/서울역사")
							.pickTopK("sub_sta_sn:a", 10)
							.build();
		
		_test1(plan);
		_test1(TestUtils.toProtoAndFromIt(plan));
	}
	
	private void _test1(Plan plan) {	
		RecordSet rset = s_marmot.executeLocally(plan);
		
		long count = rset.count();
		Assert.assertEquals(10, count);
		
		int[] values = s_marmot.executeLocally(plan)
								.fstream()
								.mapToInt(r -> r.getInt("sub_sta_sn"))
								.toArray();
		for ( int i =1; i < values.length; ++i ) {
			Assert.assertTrue(values[i] >= values[i-1]);
		}
	}
}
