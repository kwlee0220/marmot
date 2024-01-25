package marmot.optor.support;


import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mvel2.CompileException;
import org.mvel2.PropertyAccessException;

import com.google.common.collect.Maps;

import marmot.Record;
import marmot.RecordSchema;
import marmot.support.DefaultRecord;
import marmot.type.DataType;
import utils.script.MVELScript;
import utils.script.MVELScriptExecution;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ColumnVariableResolverFactoryTest {
	private static RecordSchema SCHEMA;
	private static Map<String,Object> ARGS = Maps.newHashMap();
	private ColumnVariableResolverFactory m_factReadonly;
	private ColumnVariableResolverFactory m_factUpdateable;
	private Record m_record;
	private Record m_record2;

	@BeforeClass
	public static void setupClass() {
		SCHEMA = RecordSchema.builder()
								.addColumn("col1", DataType.STRING)
								.addColumn("COL2", DataType.INT)
								.addColumn("Col3", DataType.DOUBLE)
								.build();

		ARGS.put("arg1", "AAA");
		ARGS.put("arg2", 100);
		ARGS.put("arg3", 12.5);
	}
	
	@Before
	public void setup() {
		m_factReadonly = new ColumnVariableResolverFactory(SCHEMA, ARGS).readOnly(true);
		m_factUpdateable = new ColumnVariableResolverFactory(SCHEMA, ARGS);
		
		m_record = DefaultRecord.of(SCHEMA);
		m_record.set("coL1", "aaa");
		m_record.set("cOL2", 1);
		m_record.set("COl3", 1.0);
		
		m_record2 = DefaultRecord.of(SCHEMA);
		m_record2.set("cOL1", "bbb");
		m_record2.set("cOl2", 2);
		m_record2.set("cOl3", 5.0);
	}
	
	@Test
	public void test0() throws Exception {
		MVELScript script = MVELScript.of("COL1 + 'cc'");
		MVELScript script2 = MVELScript.of("COL2 * 3");
		MVELScript script3 = MVELScript.of("col3 + col2");
		
		m_factReadonly.bind(m_record);
		Assert.assertEquals("aaacc", MVELScriptExecution.of(script).run(m_factReadonly));
		Assert.assertEquals(3, MVELScriptExecution.of(script2).run(m_factReadonly));
		Assert.assertEquals(2.0, MVELScriptExecution.of(script3).run(m_factReadonly));
	}
	
	@Test
	public void test1() throws Exception {
		MVELScript script = MVELScript.of("COL1 + arg1");
		MVELScript script2 = MVELScript.of("COL2 + arg2");
		MVELScript script3 = MVELScript.of("col3 + arg3");
		
		m_factReadonly.bind(m_record);
		Assert.assertEquals("aaaAAA", MVELScriptExecution.of(script).run(m_factReadonly));
		Assert.assertEquals(101, MVELScriptExecution.of(script2).run(m_factReadonly));
		Assert.assertEquals(13.5, MVELScriptExecution.of(script3).run(m_factReadonly));
	}
	
	@Test
	public void test2() throws Exception {
		MVELScript script = MVELScript.of("$temp1 = 'ddd'; col1+$temp1");
		
		m_factReadonly.bind(m_record);
		Assert.assertEquals("aaaddd", MVELScriptExecution.of(script).run(m_factReadonly));
	}
	
	@Test(expected=PropertyAccessException.class)
	public void test3() throws Exception {
		MVELScript script = MVELScript.of("COL4");
		
		m_factReadonly.bind(m_record);
		Assert.assertEquals("aaaAAA", MVELScriptExecution.of(script).run(m_factReadonly));
	}
	
	@Test(expected=PropertyAccessException.class)
	public void test4() throws Exception {
		MVELScript script = MVELScript.of("COL1 + Arg1");
		
		m_factReadonly.bind(m_record);
		Assert.assertEquals("aaaAAA", MVELScriptExecution.of(script).run(m_factReadonly));
	}
	
	@Test(expected=CompileException.class)
	public void test5() throws Exception {
		MVELScript script = MVELScript.of("COL1 + $temp1");
		
		m_factReadonly.bind(m_record);
		Assert.assertEquals("aaaAAA", MVELScriptExecution.of(script).run(m_factReadonly));
	}

	@Test(expected=IllegalStateException.class)
	public void test6() throws Exception {
		MVELScript script = MVELScript.of("col1 = 'kkk'");
		
		m_factReadonly.bind(m_record);
		MVELScriptExecution.of(script).run(m_factReadonly);
	}

	@Test
	public void test7() throws Exception {
		MVELScript script = MVELScript.of("col1 = 'kkk'");
		
		m_factUpdateable.bind(m_record);
		MVELScriptExecution.of(script).run(m_factUpdateable);
		Assert.assertEquals("kkk", MVELScriptExecution.of(script).run(m_factUpdateable));
		Assert.assertEquals("kkk", m_record.get("COL1"));
	}

	@Test(expected=RuntimeException.class)
	public void test8() throws Exception {
		MVELScript script = MVELScript.of("arg5 = 10");

		m_factUpdateable.bind(m_record);
		Assert.assertEquals(10, MVELScriptExecution.of(script).run(m_factUpdateable));;
	}
}
