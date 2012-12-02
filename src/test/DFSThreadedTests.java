package test;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import common.DFileID;

import dfs.DFS;


public class DFSThreadedTests {
    private DFS dfs;

    @Before
    public void setUp() throws Exception {
        dfs = new DFS(true);
        dfs.init();
    }

    @Test
    public void testMultipleFileCreation() throws InterruptedException
    {
        Thread[] threads = new Thread[20];
        final Set<Integer> fileIds = new HashSet<Integer>();
        for (int i = 0; i < 20; i++) {
            threads[i] = new Thread() {
                    public void run() {
                        fileIds.add(dfs.createDFile().id());
                    }
                };
            threads[i].start();
        }
        for (int i = 0; i < 20; i++) {
            threads[i].join();
        }
        assertEquals(20, fileIds.size());
    }

    @Test
    public void testMultipleSimultaneousWrites() throws InterruptedException
    {
        Thread[] threads = new Thread[20];
        final DFileID dFID = dfs.createDFile();
        final Random rand = new Random();
        final Set<Integer> fileContents = new HashSet<Integer>();
        for (int i = 0; i < 20; i++) {
            threads[i] = new Thread() {
                    public void run() {
                        int contents = rand.nextInt(1000000);
                        fileContents.add(contents);
                        ByteBuffer b = ByteBuffer.allocate(4);
                        b.putInt(contents);
                        dfs.write(dFID, b.array(), 0, 4);
                        //System.out.println(j);
                    }
                };
            threads[i].start();
        }
        for (int i = 0; i < 20; i++) {
            threads[i].join();
        }
        assertEquals(4, dfs.sizeDFile(dFID));
        byte[] buffer = new byte[4];
        dfs.read(dFID, buffer, 0, 4);
        assert(fileContents.contains(ByteBuffer.wrap(buffer).getInt()));
    }

    @Test
    public void testMultipleReaderThreads() throws InterruptedException {
        Thread[] threads = new Thread[20];
        final DFileID dFID = dfs.createDFile();
        for (int i = 0; i < 20; i++) {
            threads[i] = new Thread() {
                    public void run() {
                        byte[] contents = new byte[4];
                        dfs.read(dFID, contents, 0, 4);
                        int value = ByteBuffer.wrap(contents).getInt();
                        System.out.println(value);
                        assert(value == 0 || value == 42);
                    }
                };
            threads[i].start();
        }
        ByteBuffer b = ByteBuffer.allocate(4);
        b.putInt(42);
        dfs.write(dFID, b.array(), 0, 4);
        for (int i = 0; i < 20; i++) {
            threads[i].join();
        }
    }
}