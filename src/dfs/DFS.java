package dfs;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import common.Constants;
import common.DFileID;
import dblockcache.DBuffer;
import dblockcache.DBufferCache;

public abstract class DFS {
		
	private boolean _format;
	private String _volName;
	private ArrayList<Integer> _freeINodes;
	private ArrayList<Integer> _freeBlocks;
	private ArrayList<Integer> _DFileList;//partition at 512
	private DBufferCache _DBufferCache;

	DFS(String volName, boolean format) {
		_volName = volName;
		_format = format;
		initialize();
	}
	
	DFS(boolean format) {
		this(Constants.vdiskName,format);
	}

	DFS() {
		this(Constants.vdiskName,false);
	}

	/*
	 * If format is true, the system should format the underlying disk contents,
	 * i.e., initialize to empty. On success returns true, else return false.
	 */
	public boolean format(){
		if (_format){
			
		}
		else
			return false;
		return false;
	}
	
	//scan VDF to form the metadata stuctures
	private void initialize(){
		
	}
	
	/* creates a new DFile and returns the DFileID, which is useful to uniquely identify the DFile*/
	public abstract DFileID createDFile();
	
	/* destroys the file specified by the DFileID */
	public void destroyDFile(int dFID){
		_DFileList.remove(dFID);
		 DBuffer iNodeBuff = _DBufferCache.getBlock(dFID);
		 ArrayList<Integer> nodeInts = parseINode(iNodeBuff);
		_freeINodes.remove(dFID);
		
		
		
		//zero out the file and its inode, update freelists
	}

	private ArrayList<Integer> parseINode(DBuffer inode) {
			
		return null;
	}

	/*
	 * reads the file dfile named by DFileID into the buffer starting from the
	 * buffer offset startOffset; at most count bytes are transferred
	 */
	public int read(int dFID, byte[] buffer, int startOffset, int count){
		DBuffer toRead = _DBufferCache.getBlock(dFID);
		return 0;
	}
		
	
	/*
	 * writes to the file specified by DFileID from the buffer starting from the
	 * buffer offset startOffsetl at most count bytes are transferred
	 */
	public int write(int dFID, byte[] buffer, int startOffset, int count){
		DBuffer toWrite = _DBufferCache.getBlock(dFID);
		return 0;
		
	}
	
	/* returns the size in bytes of the file indicated by DFileID. */
	public int sizeDFile(int dFID){
		DBuffer toSize = _DBufferCache.getBlock(dFID);
		return 0;
	}

	/* 
	 * List all the existing DFileIDs in the volume
	 */
	public List<Integer> listAllDFiles(){
		return _DFileList;
	}
}