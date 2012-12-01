package test;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import common.Constants;

import dfs.DFS;

public class DFSTest {
	private DFS dfs;
	
	@Before
	public void setUp() throws Exception {
		dfs = new DFS();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testDiskIsFormatted() {
		assert(dfs.listAllDFiles().isEmpty());
	}
	
	@Test
	public void testCreateFile() {
		int dfid = dfs.createDFile();
		assertEquals(1, dfs.listAllDFiles().size());
		assertEquals(0, dfs.sizeDFile(dfid));
	}
	
	@Test
	public void testReadWrite() {
		int dfid = dfs.createDFile();
		byte[] input = new byte[64];
		for (int i = 0; i < 32; i++)
			input[8 + i] = (byte) i;
		dfs.write(dfid, input, 8, 32);
		
		byte[] output = new byte[64];
		dfs.read(dfid, output, 0, 64);
		assertEquals(input, output);
	}
	
	@Test
	public void testWriteOverBlockBoundary() {
		int dfid = dfs.createDFile();
		byte[] input = new byte[2 * Constants.BLOCK_SIZE];
		for (int i = 0; i < 2 * Constants.BLOCK_SIZE; i++)
			input[i] = (byte) i;
		dfs.write(dfid, input, 0, 2 * Constants.BLOCK_SIZE);
		
		byte[] output = new byte[2 * Constants.BLOCK_SIZE];
		dfs.read(dfid, output, 0, 2 * Constants.BLOCK_SIZE);
		assertEquals(input, output);
	}
	
	@Test
	public void testDestroyFile() {
		int dfid = dfs.createDFile();
		assertEquals(1, dfs.listAllDFiles().size());
		dfs.destroyDFile(dfid);
		assertEquals(0, dfs.listAllDFiles().size());
	}
}
