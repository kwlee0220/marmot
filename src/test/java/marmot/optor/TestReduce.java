package marmot.optor;

import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.MAX;
import static marmot.optor.AggregateFunction.MIN;
import static marmot.optor.AggregateFunction.STDDEV;
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


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestReduce {
	private static MarmotCore s_marmot;
	
	@BeforeClass
	public static void setupForClass() throws Exception {
		s_marmot = new MarmotCoreBuilder().forLocalMR().build();
	}
	
	@Test
	public void test0() throws Exception {
		Plan plan = Plan.builder("test")
							.loadMarmotFile("database/heap/교통/지하철/서울역사")
							.aggregate(COUNT(), SUM("sub_sta_sn"),
										MAX("kor_sub_nm").as("last"),
										MIN("kor_sub_nm").as("first"),
										AVG("sub_sta_sn").as("avg"),
										STDDEV("sub_sta_sn").as("std_dev"))
							.build();
		
		_test0(plan);
		_test0(TestUtils.toProtoAndFromIt(plan));
	}
	
	private void _test0(Plan plan) {	
		RecordSet rset = s_marmot.executeLocally(plan);
		
		long count = rset.count();
		Assert.assertEquals(1, count);
		
		rset = s_marmot.executeLocally(plan);
		for ( Record record: rset.toList() ) {
			Assert.assertEquals(286, record.getLong("count"));
			Assert.assertEquals(52361, record.getLong("sum"));
			Assert.assertEquals("효창공원앞역", record.getString("last"));
			Assert.assertEquals("가락시장역", record.getString("first"));
		}
	}
}
