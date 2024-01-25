package marmot.io.serializer;


import static org.hamcrest.Matchers.is;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;

import org.hsqldb.lib.DataOutputStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@RunWith(MockitoJUnitRunner.class)
public class EnvelopeSerializerTest {
	private EnvelopeSerializer m_serializer;
	
	@Before
	public void setup() {
		m_serializer = new EnvelopeSerializer();
	}
	
	@Test
	public void test01() throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(baos);
		
		Envelope envl1 = new Envelope(new Coordinate(10, 20));
		Envelope envl2 = new Envelope(new Coordinate(30, 40));
		
		m_serializer.serialize(envl1, out);
		m_serializer.serialize(envl2, out);
		out.close();
		
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		DataInputStream dis = new DataInputStream(bais);
		
		Envelope denvl1 = m_serializer.deserialize(dis);
		Envelope denvl2 = m_serializer.deserialize(dis);
		Assert.assertThat(denvl1, is(envl1));
		Assert.assertThat(denvl2, is(envl2));
	}
	
	@Test
	public void test02() throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(baos);
		
		Envelope envl1 = new Envelope(new Coordinate(10, 20), new Coordinate(11, 21));
		Envelope envl2 = new Envelope(new Coordinate(30, 40), new Coordinate(31, 41));
		
		m_serializer.serialize(envl1, out);
		m_serializer.serialize(envl2, out);
		out.close();
		
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		DataInputStream dis = new DataInputStream(bais);
		
		Envelope denvl1 = m_serializer.deserialize(dis);
		Envelope denvl2 = m_serializer.deserialize(dis);
		Assert.assertThat(denvl1, is(envl1));
		Assert.assertThat(denvl2, is(envl2));
	}
	
	@Test
	public void test03() throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(baos);
		
		Envelope envl = new Envelope();
		m_serializer.serialize(envl, out);
		out.close();
		
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		DataInputStream dis = new DataInputStream(bais);
		
		Envelope denvl = m_serializer.deserialize(dis);
		Assert.assertThat(denvl, is(envl));
	}
	
	@Test
	public void test04() throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(baos);
		
		Envelope envl = null;
		m_serializer.serialize(envl, out);
		out.close();
		
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		DataInputStream dis = new DataInputStream(bais);
		
		Envelope denvl = m_serializer.deserialize(dis);
		Assert.assertThat(denvl, is(envl));
	}
}
