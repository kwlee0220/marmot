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
public class TestDrop {
	private static MarmotCore s_marmot;
	
	@BeforeClass
	public static void setupForClass() throws Exception {
		s_marmot = new MarmotCoreBuilder().forLocalMR().build();
	}
	
	@Test
	public void test0() throws Exception {
		Plan plan = Plan.builder("test")
							.loadMarmotFile("database/heap/교통/지하철/서울역사")
							.drop(5)
							.build();
		
		_test0(plan);
		_test0(TestUtils.toProtoAndFromIt(plan));
	}
	
	private void _test0(Plan plan) {	
		RecordSet rset = s_marmot.executeLocally(plan);
		
		long count = rset.count();
		Assert.assertEquals(286-5, count);
	}
}
