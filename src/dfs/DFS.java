package dfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import virtualdisk.VirtualDisk;

import common.Constants;
import common.DFileID;

import dblockcache.DBuffer;
import dblockcache.DBufferCache;

public abstract class DFS {

	private String myVolName;
	private ArrayList<Integer> myFreeINodes;
	private ArrayList<Integer> myFreeBlocks;
	private TreeMap<Integer,Pair> myDFileList;// partition at 512
	private DBufferCache myDBCache;

	DFS(String volName, boolean format) {
		myVolName = volName;
		try {
			myDBCache = new DBufferCache(Constants.CACHE_SIZE, new VirtualDisk(
					myVolName, format));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}// this must be wrong since need VDF to be maintained session to
			// session?
		initializeMData();
	}

	DFS(boolean format) {
		this(Constants.vdiskName, format);
	}

	DFS() {
		this(Constants.vdiskName, false);
	}

	/*
	 * If format is true, the system should format the underlying disk contents,
	 * i.e., initialize to empty. On success returns true, else return false.
	 */

	public boolean format() {
		return false; // Don't think this is necessary with the VirtualDisk
						// implementation that formatstores at bootup.
	}

	// scan VDF to form the metadata stuctures
	private void initializeMData() {
		formatFreeBlockList(); // set free block list to all free blocks

		for (int i = 0; i < Constants.NUM_OF_INODES; i++) {
			ArrayList<Integer> iNodeInfo = parseINode(new DFileID(i));
			if (iNodeInfo.get(0) == 0) {
				myFreeINodes.add(i);
			} else {
				myDFileList.put(i, new Pair(true,0));
				for (int blockID = 1; blockID < iNodeInfo.size(); blockID++) {
					if (iNodeInfo.get(blockID) != 0)
						myFreeBlocks.remove(blockID);
				}
			}
		}

	}

	private void formatFreeBlockList() {
		for (int i = Constants.NUM_OF_INODES + 1; i < Constants.NUM_OF_BLOCKS; i++) {
			myFreeBlocks.add(i);
		}
	}

	private ArrayList<Integer> parseINode(DFileID dFID) {
		byte[] buffer = new byte[Constants.BLOCK_SIZE];
		ArrayList<Integer> parsedINode = new ArrayList<Integer>();
		DBuffer blockToParse = myDBCache.getBlock((dFID.block())
				/ Constants.BLOCK_SIZE);
		blockToParse.read(buffer, 0, Constants.BLOCK_SIZE);
		blockToParse.release();
		for (int loc = dFID.offset(); loc < Constants.INODE_SIZE; loc += 4)
			parsedINode.add(ByteBuffer.wrap(
					Arrays.copyOfRange(buffer, loc, loc + 4)).getInt());

		return parsedINode;
	}

	/*
	 * creates a new DFile and returns the DFileID, which is useful to uniquely
	 * identify the DFile
	 */
	public synchronized int createDFile() {
		sortMetadata();
		DFileID newFile = new DFileID(myFreeINodes.get(0));
		myFreeINodes.remove(0);
		myDFileList.put(newFile.id(), new Pair(true,1));
		byte[] buffer = new byte[Constants.BLOCK_SIZE];
		DBuffer container = myDBCache.getBlock(newFile.block());
		container.read(buffer, 0, Constants.BLOCK_SIZE);
		container.release();
		ByteBuffer b = ByteBuffer.allocate(Constants.INODE_SIZE);
		int nextBlock = myFreeBlocks.get(0);
		myFreeBlocks.remove(0);
		b.putInt(1);
		b.putInt(nextBlock);
		for (int i = 0; i < Constants.INODE_SIZE - 8; i += 4) {
			b.putInt(0);
		}
		byte[] newINode = b.array();
		for (int j = 0; j < Constants.INODE_SIZE; j++) {
			buffer[j + newFile.offset()] = newINode[j];
		}
		container = myDBCache.getBlock(newFile.block());
		container.write(buffer, 0, Constants.BLOCK_SIZE);
		container.release();
		myDFileList.get(newFile).numReaders--;
		return newFile.id();
	}

	/* destroys the file specified by the DFileID */
	public void destroyDFile(int dFID) {
		ArrayList<Integer> iNodeInfo = parseINode(new DFileID(dFID));

		formatBlock(dFID);
		myFreeINodes.remove(myFreeINodes.indexOf(dFID));
		synchronized(this){
		while(!myDFileList.get(dFID).isShared){
			try {
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		myDFileList.get(dFID).isShared=false;
		while(myDFileList.get(dFID).numReaders==0){
			try {
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		myDFileList.remove(dFID);
		myDFileList.get(dFID).isShared=true;
		}
		for (int i = 1; i < iNodeInfo.size(); i++) {
			myFreeBlocks.remove(myFreeBlocks.indexOf(iNodeInfo.get(i)));
			formatBlock(iNodeInfo.get(i));
		}
	}

	/* Zeros out a block */
	private void formatBlock(int dFID) {
		byte[] zeroBuffer = new byte[Constants.BLOCK_SIZE];
		for (byte b : zeroBuffer) {
			zeroBuffer[b] = 0;
		}
		DBuffer iNodeToKill = myDBCache.getBlock(dFID);
		iNodeToKill.write(zeroBuffer, 0, Constants.BLOCK_SIZE);
		iNodeToKill.release();
	}

	/*
	 * reads the file dfile named by DFileID into the buffer starting from the
	 * buffer offset startOffset; at most count bytes are transferred
	 */
	public int read(int dFID, byte[] buffer, int startOffset, int count) {
		ArrayList<Integer> iNodeInfo = parseINode(new DFileID(dFID));
		int bytesRead = 0;

		for (int i = 1; i <= iNodeInfo.get(0); i++) {
			DBuffer toRead = myDBCache.getBlock(iNodeInfo.get(i));
			bytesRead += toRead.read(buffer, startOffset + bytesRead, count
					- bytesRead);
			toRead.release();
			if (bytesRead >= count)
				break;
		}
		return bytesRead;
	}

	/*
	 * writes to the file specified by DFileID from the buffer starting from the
	 * buffer offset startOffsetl at most count bytes are transferred
	 */
	public int write(int dFID, byte[] buffer, int startOffset, int count) {
		sortMetadata();
		if (myFreeBlocks.size() == 0) {
			System.out.println("VOLUME SIZE EXCEEDED");
			return -1;
		}
		DFileID file = new DFileID(dFID);
		ArrayList<Integer> iNodeInfo = parseINode(file);
		int bytesWritten = 0;
		int vBlockNumber = 1;
		while (bytesWritten < count
				&& buffer.length > (startOffset + bytesWritten)) {
			DBuffer nextBlock;
			if (vBlockNumber < iNodeInfo.get(0)) {
				nextBlock = myDBCache.getBlock(iNodeInfo.get(vBlockNumber));
				vBlockNumber++;
			} else {
				DBuffer iNodeBlock = myDBCache.getBlock(file.block());
				byte[] blockBuffer = new byte[Constants.BLOCK_SIZE];
				iNodeBlock.read(buffer, 0, Constants.BLOCK_SIZE);
				ByteBuffer b = ByteBuffer.allocate(Constants.INODE_SIZE);
				int nextPBlockNumber;
				synchronized (this) {
					nextPBlockNumber = myFreeBlocks.get(0);
					myFreeBlocks.remove(0);
				}
				iNodeInfo.set(0, iNodeInfo.get(0) + 1);
				iNodeInfo.add(nextPBlockNumber);
				try {
					for (int i : iNodeInfo)
						b.putInt(i);

				} catch (BufferOverflowException e) {
					System.out.println("MAX FILE SIZE EXCEEDED");
					return -1;
				}
				for (int j = 0; j < Constants.INODE_SIZE; j++) {
					blockBuffer[j + file.offset()] = b.array()[j];
				}
				iNodeBlock.write(blockBuffer, 0, Constants.BLOCK_SIZE);
				iNodeBlock.release();
				nextBlock = myDBCache.getBlock(nextPBlockNumber);
				vBlockNumber++;
			}
			bytesWritten += nextBlock.write(buffer, startOffset + bytesWritten,
					count - bytesWritten);
			nextBlock.release();
		}
		return count;
	}

	/* returns the size in bytes of the file indicated by DFileID. */
	public int sizeDFile(int dFID) {
		ArrayList<Integer> iNodeInfo = parseINode(new DFileID(dFID));
		return iNodeInfo.get(0)*Constants.BLOCK_SIZE;
	}

	/*
	 * List all the existing DFileIDs in the volume
	 */

	public void sync() {
		myDBCache.sync();
	}

	private void sortMetadata() {
		Collections.sort(myFreeINodes);
		Collections.sort(myFreeBlocks);
	}

	private class Pair{
		boolean isShared;
		int numReaders;
		private Pair(boolean shared, int readers){
		isShared=shared;
		numReaders=readers;
		}
	}
}
