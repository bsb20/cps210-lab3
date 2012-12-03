package test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.junit.Before;
import org.junit.Test;

import common.Constants;


import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import common.Constants;

import common.DFileID;


import dfs.DFS;


public class PersistenceTests {
	private DFS dfs;

	@Before
	public void setUp() throws IOException {
		copyFile("test_disk.dat", "DSTORE.dat");
		dfs = new DFS(false);
		dfs.init();
	}
	
	public void persistingOfWrites() {
		DFileID originalFile = dfs.createDFile();
		ByteBuffer b=ByteBuffer.allocate(4);
		b.putInt(333);
		dfs.write(originalFile, b.array(), 0, 4);
		for(int i=0; i<30; i++){
			DFileID newFile= dfs.createDFile();
			ByteBuffer contents=ByteBuffer.allocate(50*Constants.BLOCK_SIZE);
			for(int j=0; j<50*Constants.BLOCK_SIZE/4; j++){
				contents.putInt(1);
			}
			dfs.write(newFile, contents.array(), 0, 50*Constants.BLOCK_SIZE);
		}
		byte[] result=new byte[4];
		dfs.read(originalFile, result, 0, 4);
		assert(333==ByteBuffer.wrap(result).getInt());
	}
	
	@Test
	public void testPersistenceOfFiles() {
		assertEquals(1, dfs.listAllDFiles().size());
	}
	
	@Test
	public void testPersistenceOfReads() {
		DFileID dfid = new DFileID(1);
		byte[] input = new byte[64];
		for (int i = 0; i < 32; i++)
			input[8 + i] = (byte) i;

		byte[] output = new byte[64];
		dfs.read(dfid, output, 8, 56);
		assertArrayEquals(input, output);
	}
	
	private void copyFile(String srcLocation, String dstLocation) throws IOException {
        File src = new File(srcLocation);
        File dst = new File(dstLocation);

        InputStream inStream = new FileInputStream(src);
        OutputStream outStream = new FileOutputStream(dst);

        byte[] buffer = new byte[Constants.NUM_OF_BLOCKS * Constants.BLOCK_SIZE];

        int length;
        while ((length = inStream.read(buffer)) > 0){
            outStream.write(buffer, 0, length);
        }

        inStream.close();
        outStream.close();
	}
	
}
