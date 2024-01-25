package marmot.optor.support;


import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import marmot.MarmotCore;
import marmot.MarmotCoreBuilder;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordScript;
import marmot.optor.ScriptFilter;
import marmot.support.DefaultRecord;
import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@RunWith(MockitoJUnitRunner.class)
public class ScriptRecordPredicateTest {
	private static MarmotCore s_marmot;
	private static RecordSchema SCHEMA;
	private Record m_record;
	
	@BeforeClass
	public static void setupClass() {
		s_marmot = new MarmotCoreBuilder().forLocal().build();
		SCHEMA = RecordSchema.builder()
							.addColumn("a", DataType.INT)
							.build();
	}
	
	@Before
	public void setup() {
		m_record = DefaultRecord.of(SCHEMA);
	}
	
	@Test
	public void test0() throws Exception {
		ScriptFilter pred = new ScriptFilter(RecordScript.of("a == 1"));
		pred.initialize(s_marmot, m_record.getRecordSchema());
		
		m_record.set("a", 1);
		Assert.assertEquals(true, pred.test(m_record));
		
		m_record.set("a", 2);
		Assert.assertEquals(false, pred.test(m_record));
	}
	
	@Test
	public void test1() throws Exception {
		ScriptFilter pred = new ScriptFilter(RecordScript.of("$b = 1", "a == $b"));
		pred.initialize(s_marmot, SCHEMA);
		
		m_record.set("a", 1);
		Assert.assertEquals(true, pred.test(m_record));
		
		m_record.set("a", 2);
		Assert.assertEquals(false, pred.test(m_record));
	}
	
	@Test
	public void test2() throws Exception {
		ScriptFilter pred = new ScriptFilter(RecordScript.of("$x = 10; a == 1"));
		pred.initialize(s_marmot, m_record.getRecordSchema());
		
		m_record.set("a", 1);
		Assert.assertEquals(true, pred.test(m_record));
		
		m_record.set("a", 2);
		Assert.assertEquals(false, pred.test(m_record));
	}
}
