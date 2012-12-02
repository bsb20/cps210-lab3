import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import dfs.DFS;


public class PersistenceTests {

	@Before
	public void setUp() {
		copyFile("test_disk.dat", "DSTORE.dat");
		DFS dfs = new DFS(false);
		dfs.init();
	}
	
	@Test
	public void test() {
		fail("Not yet implemented");
	}

	public void copyFile(String src, String dst) {
		
	}
}
