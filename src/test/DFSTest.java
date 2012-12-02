package test;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import common.Constants;
import common.DFileID;

import dfs.DFS;

public class DFSTest {
	private DFS dfs;

	@Before
	public void setUp() throws Exception {
		dfs = new DFS();
		dfs.init();
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
		DFileID dfid = dfs.createDFile();
		assertEquals(1, dfs.listAllDFiles().size());
		assertEquals(0, dfs.sizeDFile(dfid));
	}

	@Test
	public void testReadWrite() {
		DFileID dfid = dfs.createDFile();
		byte[] input = new byte[64];
		for (int i = 0; i < 32; i++)
			input[8 + i] = (byte) i;
		dfs.write(dfid, input, 8, 32);

		byte[] output = new byte[64];
		dfs.read(dfid, output, 8, 56);
		assertArrayEquals(input, output);
	}

    @Test
    public void testFileSize() {
		DFileID dfid = dfs.createDFile();
		byte[] input = new byte[64];
		for (int i = 0; i < 32; i++)
			input[8 + i] = (byte) i;
		dfs.write(dfid, input, 8, 37);
        assertEquals(37, dfs.sizeDFile(dfid));
    }

	@Test
	public void testWriteOverBlockBoundary() {
		DFileID dfid = dfs.createDFile();
		byte[] input = new byte[2 * Constants.BLOCK_SIZE];
		for (int i = 0; i < 2 * Constants.BLOCK_SIZE; i++)
			input[i] = (byte) i;
		dfs.write(dfid, input, 0, 2 * Constants.BLOCK_SIZE);

		byte[] output = new byte[2 * Constants.BLOCK_SIZE];
		dfs.read(dfid, output, 0, 2 * Constants.BLOCK_SIZE);
		assertArrayEquals(input, output);
	}

    @Test
    public void testWriteTwoFiles() {
		DFileID file1 = dfs.createDFile();
		DFileID file2 = dfs.createDFile();
        assert(file1.id() > file2.id());
    }

	@Test
	public void testDestroyFile() {
		DFileID dfid = dfs.createDFile();
		assertEquals(1, dfs.listAllDFiles().size());
		dfs.destroyDFile(dfid);
		assertEquals(0, dfs.listAllDFiles().size());
	}

    @Test
    public void testDestroyFileReleasesInode() {
		DFileID file1 = dfs.createDFile();
		dfs.destroyDFile(file1);
        DFileID file2 = dfs.createDFile();
        assertEquals(file1.id(), file2.id());
    }
}
