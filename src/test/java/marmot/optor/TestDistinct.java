package marmot.optor;


import java.util.Set;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import marmot.MarmotCore;
import marmot.MarmotCoreBuilder;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.TestUtils;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestDistinct {
	private static MarmotCore s_marmot;
	
	@BeforeClass
	public static void setupForClass() throws Exception {
		s_marmot =new MarmotCoreBuilder().forLocalMR().build();
	}
	
	@Test
	public void test0() throws Exception {
		Plan plan;
		plan = Plan.builder("test")
						.load("교통/지하철/서울역사")
						.distinct("SIG_CD")
						.build();
		
		_test0(plan);
		_test0(TestUtils.toProtoAndFromIt(plan));
	}
	
	private void _test0(Plan plan) {	
		RecordSet rset = s_marmot.executeLocally(plan);
		
		RecordSchema schema = rset.getRecordSchema();
		RecordSchema srcSchema = s_marmot.getDataSet("교통/지하철/서울역사").getRecordSchema();
		Assert.assertEquals(srcSchema, schema);
		
		Set<String> sigCds = rset.fstream().map(r -> r.getString("sig_cd")).toSet();
		Set<String> srcSigCd = calcDistinctSigCds();
		Assert.assertEquals(srcSigCd, sigCds);
	}
	
	private Set<String> calcDistinctSigCds() {
		Plan plan;
		
		plan = Plan.builder("prepare")
						.load("교통/지하철/서울역사")
						.project("SIG_CD")
						.build();
		return s_marmot.executeLocally(plan)
						.fstream()
						.map(r -> r.getString(0))
						.distinct()
						.toSet();
	}
}
