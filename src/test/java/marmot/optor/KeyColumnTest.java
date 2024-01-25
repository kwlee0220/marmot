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
public class KeyColumnTest {
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
		KeyColumn kc1 = KeyColumn.of("col1");
		KeyColumn kc2 = KeyColumn.of("cOl1");
		
		Assert.assertEquals(kc2, kc1);
		Assert.assertEquals(true, kc1.matches("COL1"));
		Assert.assertEquals(false, kc1.matches("KOL1"));
		Assert.assertEquals(kc2.hashCode(), kc1.hashCode());

		Assert.assertEquals("col1", kc1.toString());
		Assert.assertEquals(KeyColumn.fromString(kc2.toString()), kc1);
	}
	
	@Test
	public void test02() throws Exception {
		KeyColumn kc1 = KeyColumn.of("col1", SortOrder.ASC, NullsOrder.LAST);
		KeyColumn kc2 = KeyColumn.of("Col1", SortOrder.DESC, NullsOrder.LAST);
		KeyColumn kc3 = KeyColumn.of("cOl1", SortOrder.ASC, NullsOrder.LAST);
		
//		Assert.assertNotEquals(kc2, kc1);
		Assert.assertEquals(kc3, kc1);
		Assert.assertEquals(true, kc2.matches("COL1"));
		Assert.assertEquals(false, kc1.matches("KOL1"));
//		Assert.assertNotEquals(kc2.hashCode(), kc1.hashCode());
		Assert.assertEquals(kc3.hashCode(), kc1.hashCode());

		Assert.assertEquals("col1:ASC:LAST", kc1.toString());
		Assert.assertEquals(KeyColumn.fromString("col1:ASC:LAST"), kc3);
		Assert.assertEquals("Col1:DESC:LAST", kc2.toString());
		Assert.assertEquals(KeyColumn.fromString("Col1:D:L"), kc2);
	}
}
