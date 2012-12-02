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

import dfs.DFS;


public class PersistenceTests {

	@Before
	public void setUp() throws IOException {
		copyFile("test_disk.dat", "DSTORE.dat");
		DFS dfs = new DFS(false);
		dfs.init();
	}

	@Test
	public void test() {

	}

	public void copyFile(String srcLocation, String dstLocation) throws IOException {
        File src =new File(srcLocation);
        File dst =new File(dstLocation);

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
