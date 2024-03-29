package common;
/*
 * This class contains the global constants used in DFS
 */

public class Constants {

	public static final int NUM_OF_BLOCKS = 16384; // 2^14
	public static final int BLOCK_SIZE = 1024; // 1kB
	public static final int NUM_OF_INODES = 512; //max of 512 files 
	public static final int CACHE_SIZE = 1024; //guess from Ian 
	public static final int INODE_SIZE=256;

	/* DStore Operation types */
	public enum DiskOperationType {
		READ, WRITE
	};

	/* Virtual disk file/store name */
	public static final String vdiskName = "DSTORE.dat";
}