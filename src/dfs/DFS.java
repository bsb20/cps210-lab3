package dfs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

import virtualdisk.VirtualDisk;

import common.Constants;
import common.DFileID;
import dblockcache.DBuffer;
import dblockcache.DBufferCache;

public abstract class DFS {
		
	private boolean myFormat;
	private String myVolName;
	private ArrayList<Integer> myFreeINodes;
	private ArrayList<Integer> myFreeBlocks;
	private ArrayList<Integer> myDFileList;//partition at 512
	private DBufferCache myDBCache;

	DFS(String volName, boolean format) {
		myVolName = volName;
		myFormat = format;
		myDBCache = new DBufferCache(Constants.CACHE_SIZE, new VirtualDisk(myVolName, format));
		initializeMData();//needs implementation
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
	/*
	public boolean format(){
		return false; //Don't think this is necessary with the VirtualDisk implementation that formatstores at bootup. 
	}
	*/
	
	private ArrayList<Integer> parseINode(int dFID){
		byte[] buffer = new byte[Constants.BLOCK_SIZE];
		ArrayList<Integer> parsedINode = new ArrayList<Integer>();
		
		DBuffer blockToParse = myDBCache.getBlock(dFID);
		blockToParse.read(buffer, 0, Constants.BLOCK_SIZE);
		for (int loc=0; loc<Constants.BLOCK_SIZE; loc += 4)
			parsedINode.add(java.nio.ByteBuffer.wrap(Arrays.copyOfRange(buffer, loc, loc+4)).getInt());
		
		return parsedINode;
	}
	
	//scan VDF to form the metadata stuctures
	private void initializeMData(){
		formatFreeBlocks(); //set free block list to all free blocks
		
		for (int i=0; i<Constants.NUM_OF_INODES; i++){
			ArrayList<Integer> iNodeInfo = parseINode(i);
			if (iNodeInfo.get(0) == 0){
				myFreeINodes.add(i);
			}
			else{
				myDFileList.add(i);
				for (int blockID = 1; blockID<iNodeInfo.size(); blockID++){
					if (iNodeInfo.get(blockID) != 0)
						myFreeBlocks.remove(blockID);
				}
			}
		}

	}
	
	private void formatFreeBlocks() {
		for (int i = Constants.NUM_OF_INODES+1; i<Constants.NUM_OF_BLOCKS; i++){
			myFreeBlocks.add(i);
		}
	}

	/* creates a new DFile and returns the DFileID, which is useful to uniquely identify the DFile*/
	public int createDFile(){
		sortMetadata();
		int DFileID = myFreeINodes.get(0);
		myFreeINodes.remove(0);
		return DFileID;
	}
	
	/* destroys the file specified by the DFileID */
	public void destroyDFile(int dFID){
		byte[] buffer = new byte[Constants.BLOCK_SIZE];
		DBuffer iNodeToKill = myDBCache.getBlock(dFID);
		iNodeToKill.read(buffer, 0, Constants.BLOCK_SIZE);
		//ArrayList<Integer> nodeInts = parseINode(iNodeBuff);
		for (int i=1; i < nodeInts.size(); i++)
			 myFreeBlocks.remove(myFreeBlocks.indexOf(nodeInts.get(i)));		 
		myFreeINodes.remove(myFreeINodes.indexOf(dFID));
		myDFileList.remove(myDFileList.indexOf(dFID));
	}

	private ArrayList<Integer> parseINode(DBuffer inode) {
			//break up inode - seperate into
		return null;
	}

	/*
	 * reads the file dfile named by DFileID into the buffer starting from the
	 * buffer offset startOffset; at most count bytes are transferred
	 */
	public int read(int dFID, byte[] buffer, int startOffset, int count){
		DBuffer toRead = myDBCache.getBlock(dFID);
		toRead.read(buffer, startOffset, count);
		myDBCache.releaseBlock(toRead);
		return count;
	}
		
	
	/*
	 * writes to the file specified by DFileID from the buffer starting from the
	 * buffer offset startOffsetl at most count bytes are transferred
	 */
	public int write(int dFID, byte[] buffer, int startOffset, int count){
		DBuffer toWriteINode = myDBCache.getBlock(dFID);	
		toWrite.write(buffer, startOffset, count);
		myDBCache.releaseBlock(toWrite);
		return count;
		
	}
	
	/* returns the size in bytes of the file indicated by DFileID. */
	public int sizeDFile(int dFID){
		byte[] sizeBytes = new byte[4];
		DBuffer toSize = myDBCache.getBlock(dFID);
		toSize.read(sizeBytes, 0, 4);
		int size = java.nio.ByteBuffer.wrap(sizeBytes).getInt();
		
		return size;
	}

	/* 
	 * List all the existing DFileIDs in the volume
	 */
	public List<Integer> listAllDFiles(){
		sortMetadata();
		return myDFileList;
	}
	
	public void sync(){
		myDBCache.sync();
	}
	
	private void sortMetadata(){
		Collections.sort(myDFileList);
		Collections.sort(myFreeINodes);
		Collections.sort(myFreeBlocks);
	}
	
}


