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
public class TestRank {
	private static MarmotCore s_marmot;
	
	@BeforeClass
	public static void setupForClass() throws Exception {
		s_marmot = new MarmotCoreBuilder().forLocalMR().build();
	}
	
	@Test
	public void test0() throws Exception {
		Plan plan = Plan.builder("test")
							.load("/교통/지하철/서울역사")
							.rank("sub_sta_sn:a", "rank")
							.build();
		
		_test0(plan);
		_test0(TestUtils.toProtoAndFromIt(plan));
	}
	
	private void _test0(Plan plan) {	
		RecordSet rset = s_marmot.executeLocally(plan);
		
		long count = rset.count();
		Assert.assertEquals(286, count);
		
		int i = 0;
		int[] values = new int[(int)count];
		for ( Record r: s_marmot.executeLocally(plan).toList() ) {
			long rank = r.getLong("rank");
			int sn = r.getInt("sub_sta_sn");
			
			values[i] = sn;
			if ( i > 0 ) {
				Assert.assertTrue(values[i] >= values[i-1]);
			}
			Assert.assertEquals(i, rank);
			
			++i;
		}
	}
	
	@Test
	public void test1() throws Exception {
		Plan plan = Plan.builder("test")
							.loadMarmotFile("database/heap/교통/지하철/서울역사")
							.rank("sub_sta_sn:d", "rank2")
							.build();
		
		_test1(plan);
		_test1(TestUtils.toProtoAndFromIt(plan));
	}
	
	private void _test1(Plan plan) {		
		RecordSet rset = s_marmot.executeLocally(plan);
		
		long count = rset.count();
		Assert.assertEquals(286, count);
		
		int i = 0;
		int[] values = new int[(int)count];
		for ( Record r: s_marmot.executeLocally(plan).toList() ) {
			long rank = r.getLong("rank2");
			int sn = r.getInt("sub_sta_sn");
			
			values[i] = sn;
			if ( i > 0 ) {
				Assert.assertTrue(values[i] <= values[i-1]);
			}
			Assert.assertEquals(i, rank);
			++i;
		}
	}
}
