package marmot.optor;


import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.MAX;
import static marmot.optor.AggregateFunction.MIN;
import static marmot.optor.AggregateFunction.SUM;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import marmot.MarmotCore;
import marmot.MarmotCoreBuilder;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSet;
import marmot.TestUtils;
import marmot.plan.Group;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestReduceByGroupKey {
	private static MarmotCore s_marmot;
	
	@BeforeClass
	public static void setupForClass() throws Exception {
		s_marmot = new MarmotCoreBuilder().forLocalMR().build();
	}
	
	@Test
	public void test0() throws Exception {
		Plan plan = Plan.builder("test")
							.loadMarmotFile("database/heap/교통/지하철/서울역사")
							.aggregateByGroup(Group.ofKeys("sig_cd"),
												COUNT(), SUM("sub_sta_sn"),
												MIN("kor_sub_nm").as("first"),
												MAX("kor_sub_nm").as("last"))
							.build();
		
		_test0(plan);
		_test0(TestUtils.toProtoAndFromIt(plan));
	}
	
	private void _test0(Plan plan) {	
		RecordSet rset = s_marmot.executeLocally(plan);
		
		long count = rset.count();
		Assert.assertEquals(25, count);
		
		rset = s_marmot.executeLocally(plan);
		for ( Record record: rset.toList() ) {
			System.out.println(record);
		}
	}
}
