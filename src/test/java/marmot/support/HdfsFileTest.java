package marmot.support;


import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import marmot.MarmotCore;
import marmot.MarmotCoreBuilder;
import marmot.io.HdfsPath;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@RunWith(MockitoJUnitRunner.class)
public class HdfsFileTest {
	private static Configuration s_conf;
	private HdfsPath m_tmpDir;
	private HdfsPath m_result;
	
	@BeforeClass
	public static void setupClass() throws Exception {
		MarmotCore marmot = new MarmotCoreBuilder().forLocalMR().build();
		s_conf = marmot.getHadoopConfiguration();
	}
	
	@Before
	public void setup() {
		m_tmpDir = HdfsPath.of(s_conf, new Path("tmp"));
		m_result = HdfsPath.of(s_conf, new Path("tmp/result"));
	}
	
	@Test
	public void test01() throws Exception {
		Assert.assertEquals(true, m_tmpDir.exists());
		Assert.assertEquals(true, m_tmpDir.isDirectory());
		Assert.assertEquals(false, m_result.isDirectory());
		
		Assert.assertEquals(m_result, m_tmpDir.child("result"));
		Assert.assertEquals(true, m_result.getParent().isPresent());
		Assert.assertEquals(m_tmpDir, m_result.getParent().get());
	}
	
	
	@Test(expected=IOException.class)
	public void test02() throws Exception {
		HdfsPath child = m_tmpDir.child("test2");
		
		if ( child.exists() ) {
			child.delete();
		}
		
		Assert.assertEquals(false, child.exists());
		Assert.assertEquals(true, child.mkdir());
		Assert.assertEquals(true, child.exists());
		child.mkdir();
		Assert.assertEquals(true, child.delete());
		Assert.assertEquals(false, child.exists());
	}

	@Test
	public void test03() throws Exception {
		HdfsPath child = m_tmpDir.child("test2");
		
		if ( !child.exists() ) {
			child.mkdir();
		}
		
		Assert.assertEquals(true, child.delete());
		Assert.assertEquals(false, child.exists());
	}
	
	@Test
	public void test04() throws Exception {
		HdfsPath root = m_tmpDir.child("level1");
		root.create().close();
		Assert.assertEquals(true, root.exists());
		root.delete();
		
		HdfsPath child1 = m_tmpDir.child("level1/child1");
		child1.create().close();
		Assert.assertEquals(true, child1.exists());
		child1.delete();
		Assert.assertEquals(true, root.exists());
		root.delete();
	}
	
	@Test
	public void test05() throws Exception {
		HdfsPath root = m_tmpDir.child("level1");
		
		HdfsPath child1 = m_tmpDir.child("level1/child1");
		child1.create().close();
		Assert.assertEquals(true, child1.exists());
		child1.deleteUpward();
		Assert.assertEquals(false, root.exists());
	}
	
	@Test
	public void test06() throws Exception {
		HdfsPath child1 = m_tmpDir.child("level1/child1");
		HdfsPath child2 = m_tmpDir.child("level1/child2");

		child1.create().close();
		Assert.assertEquals(true, child1.exists());
		Assert.assertEquals(false, child2.exists());
		
		child1.moveTo(child2);
		Assert.assertEquals(true, child2.exists());
		Assert.assertEquals(false, child1.exists());
		
		child2.deleteUpward();
	}
	
	@Test
	public void test07() throws Exception {
		HdfsPath child1 = m_tmpDir.child("level1/child1");
		child1.create().close();
		HdfsPath child2 = m_tmpDir.child("level1/child2");
		child2.create().close();
		
		HdfsPath root = m_tmpDir.child("level1");
		HdfsPath root2 = m_tmpDir.child("level2");
		Assert.assertEquals(true, root.exists());
		Assert.assertEquals(false, root2.exists());
		root.moveTo(root2);
		Assert.assertEquals(false, root.exists());
		Assert.assertEquals(true, root2.exists());
		
		root2.delete();
	}
	
	@Test
	public void test08() throws Exception {
		HdfsPath child1 = m_tmpDir.child("level1/child1");
		child1.create().close();
		HdfsPath child2 = m_tmpDir.child("level1/child2");
		child2.create().close();
		HdfsPath child3 = m_tmpDir.child("level1/level2/child3");
		child3.create().close();

		List<HdfsPath> files;
		HdfsPath level1 = m_tmpDir.child("level1");
		
		files = level1.walkTree(true).toList();
		Assert.assertEquals(5, files.size());
		files = level1.walkTree(false).toList();
		Assert.assertEquals(4, files.size());
		files = level1.walkRegularFileTree().toList();
		Assert.assertEquals(3, files.size());
		
		level1.delete();
	}
}
