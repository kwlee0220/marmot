package marmot.optor;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import marmot.RecordSchema;
import marmot.io.MultiColumnKey;
import marmot.type.DataType;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MultiColumnKeyTest {
	private RecordSchema m_schema;
	private MultiColumnKey m_key;
	
	@Before
	public void setup() {
		m_schema = RecordSchema.builder()
								.addColumn("col1", DataType.STRING)
								.addColumn("COL2", DataType.INT)
								.addColumn("Col3", DataType.DOUBLE)
								.build();
		m_key = MultiColumnKey.of("coL1", "COL2");
	}
	
	@Test
	public void test01() throws Exception {
		Assert.assertEquals(2, m_key.length());
		
		Assert.assertEquals(true, m_key.existsKeyColumn("col1"));
		Assert.assertEquals(true, m_key.existsKeyColumn("COl2"));
		Assert.assertEquals(false, m_key.existsKeyColumn("Col3"));
		
		Assert.assertEquals(true, m_key.getKeyColumnAt(0).matches("COL1"));
		Assert.assertEquals(true, m_key.getKeyColumnAt(1).matches("coL2"));
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void test02() throws Exception {
		m_key.getKeyColumnAt(2);
	}
	
	@Test
	public void test03() throws Exception {
		MultiColumnKey ckey = m_key.complement(m_schema);
		
		Assert.assertEquals(1, ckey.length());
		Assert.assertEquals("Col3", ckey.getKeyColumnAt(0).name());
		
		m_key = MultiColumnKey.of("coL1", "COL2", "col3");
		ckey = m_key.complement(m_schema);
		Assert.assertEquals(0, ckey.length());
		
		m_key = MultiColumnKey.of();
		ckey = m_key.complement(m_schema);
		Assert.assertEquals(3, ckey.length());
		Assert.assertEquals(true, ckey.existsKeyColumn("col1"));
		Assert.assertEquals(true, ckey.existsKeyColumn("COl2"));
		Assert.assertEquals(true, ckey.existsKeyColumn("Col3"));
	}
}
