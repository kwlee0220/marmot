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
import org.locationtech.jts.geom.Point;
import org.mockito.junit.MockitoJUnitRunner;

import marmot.geo.GeoClientUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@RunWith(MockitoJUnitRunner.class)
public class PointSerializerTest {
	private PointSerializer m_serializer;
	
	@Before
	public void setup() {
		m_serializer = new PointSerializer();
	}
	
	@Test
	public void test01() throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(baos);
		
		Point pt1 = GeoClientUtils.toPoint(new Coordinate(10, 20));
		Point pt2 = GeoClientUtils.toPoint(new Coordinate(30, 40));
		
		m_serializer.serialize(pt1, out);
		m_serializer.serialize(pt2, out);
		out.close();
		
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		DataInputStream dis = new DataInputStream(bais);
		
		Point dpt1 = m_serializer.deserialize(dis);
		Point dpt2 = m_serializer.deserialize(dis);
		Assert.assertThat(dpt1, is(pt1));
		Assert.assertThat(dpt2, is(pt2));
	}
	
	@Test
	public void test03() throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(baos);
		
		Point pt = GeoClientUtils.EMPTY_POINT;
		m_serializer.serialize(pt, out);
		out.close();
		
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		DataInputStream dis = new DataInputStream(bais);
		
		Point denvl = m_serializer.deserialize(dis);
		Assert.assertThat(denvl, is(pt));
	}
	
	@Test
	public void test04() throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(baos);
		
		Point envl = null;
		m_serializer.serialize(envl, out);
		out.close();
		
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		DataInputStream dis = new DataInputStream(bais);
		
		Point denvl = m_serializer.deserialize(dis);
		Assert.assertThat(denvl, is(envl));
	}
}
