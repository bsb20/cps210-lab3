package dfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

import virtualdisk.VirtualDisk;

import common.Constants;
import common.DFileID;

import dblockcache.DBuffer;
import dblockcache.DBufferCache;

public class DFS {
	private String myVolName;
	private TreeSet<Integer> myFreeINodes, myFreeBlocks;
	private TreeMap<DFileID, LockState> myDFiles;
	private DBufferCache myDBCache;


	public DFS(String volName, boolean format) {
		myVolName = volName;
		try {
			myDBCache = new DBufferCache(Constants.CACHE_SIZE,
                                         new VirtualDisk(myVolName, format));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}// this must be wrong since need VDF to be maintained session to
			// session?
		initializeMData();
	}

	public DFS(boolean format) {
		this(Constants.vdiskName, format);
	}

	public DFS() {
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

    public List<DFileID> listAllDFiles()
    {
        List<DFileID> dFiles = new ArrayList<DFileID>();
        dFiles.addAll(myDFiles.keySet());
        return dFiles;
    }

	/*
	 * creates a new DFile and returns the DFileID, which is useful to uniquely
	 * identify the DFile
	 */
	public synchronized DFileID createDFile() {
        int dFID = myFreeINodes.first();
        myFreeINodes.remove(dFID);
		DFileID newFile = new DFileID(dFID);
		myDFiles.put(newFile, new LockState(true, 0));

        acquireWriteLock(newFile);
		byte[] buffer = new byte[Constants.BLOCK_SIZE];
		int nextBlock = myFreeBlocks.first();
		myFreeBlocks.remove(nextBlock);

		ByteBuffer b = ByteBuffer.allocate(Constants.INODE_SIZE);
		b.putInt(1);  // number of blocks in file
		b.putInt(nextBlock);  // first block address
		byte[] newINode = b.array();

		DBuffer container = myDBCache.getBlock(newFile.block());
		container.read(buffer, 0, Constants.BLOCK_SIZE);
        overwrite(buffer, newINode, newFile.offset());
		container.write(buffer, 0, Constants.BLOCK_SIZE);
		myDBCache.releaseBlock(container);

        releaseWriteLock(newFile);
		return newFile;
	}

	/* destroys the file specified by the DFileID */
	public void destroyDFile(DFileID dFID) {
        acquireWriteLock(dFID);
		formatINode(dFID);
		myDFiles.remove(dFID);
        releaseWriteLock(dFID);
	}

	/*
	 * reads the file dfile named by DFileID into the buffer starting from the
	 * buffer offset startOffset; at most count bytes are transferred
	 */
	public int read(DFileID dFID, byte[] buffer, int startOffset, int count) {
		ArrayList<Integer> iNodeInfo = parseINode(dFID);
		int bytesRead = 0;

		for (int i = 1; i <= iNodeInfo.get(0); i++) {
			DBuffer toRead = myDBCache.getBlock(iNodeInfo.get(i));
			bytesRead += toRead.read(buffer, startOffset + bytesRead, count
					- bytesRead);
			myDBCache.releaseBlock(toRead);
			if (bytesRead >= count)
				break;
		}
		return bytesRead;
	}

	/*
	 * writes to the file specified by DFileID from the buffer starting from the
	 * buffer offset startOffsetl at most count bytes are transferred
	 */
	public int write(DFileID dFID, byte[] buffer, int startOffset, int count) {
		if (myFreeBlocks.size() == 0) {
			System.out.println("VOLUME SIZE EXCEEDED");
			return -1;
		}
		ArrayList<Integer> iNodeInfo = parseINode(dFID);
		int bytesWritten = 0;
		int vBlockNumber = 1;
		while (bytesWritten < count
				&& buffer.length > (startOffset + bytesWritten)) {
			DBuffer nextBlock;
			if (vBlockNumber < iNodeInfo.get(0)) {
				nextBlock = myDBCache.getBlock(iNodeInfo.get(vBlockNumber));
				vBlockNumber++;
			} else {
				DBuffer iNodeBlock = myDBCache.getBlock(dFID.block());
				byte[] blockBuffer = new byte[Constants.BLOCK_SIZE];
				iNodeBlock.read(buffer, 0, Constants.BLOCK_SIZE);
				ByteBuffer b = ByteBuffer.allocate(Constants.INODE_SIZE);
				int nextPBlockNumber;
				synchronized (this) {
					nextPBlockNumber = myFreeBlocks.first();
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
					blockBuffer[j + dFID.offset()] = b.array()[j];
				}
				iNodeBlock.write(blockBuffer, 0, Constants.BLOCK_SIZE);
				myDBCache.releaseBlock(iNodeBlock);
				nextBlock = myDBCache.getBlock(nextPBlockNumber);
				vBlockNumber++;
			}
			bytesWritten += nextBlock.write(buffer, startOffset + bytesWritten,
					count - bytesWritten);
			myDBCache.releaseBlock(nextBlock);
		}
		return count;
	}

	/* returns the size in bytes of the file indicated by DFileID. */
	public int sizeDFile(DFileID dFID) {
		ArrayList<Integer> iNodeInfo = parseINode(dFID);
		return iNodeInfo.get(0)*Constants.BLOCK_SIZE;
	}

	/*
	 * List all the existing DFileIDs in the volume
	 */
	public void sync() {
		myDBCache.sync();
	}

	/* Zeros out a block */
	private void formatINode(DFileID dFID) {
		DBuffer iNodeToKill = myDBCache.getBlock(dFID.block());
		byte[] blockData= new byte[Constants.BLOCK_SIZE];
		iNodeToKill.read(blockData, 0, Constants.BLOCK_SIZE);
		for(int i=0; i<Constants.INODE_SIZE; i++){
			blockData[i+dFID.offset()]=0;
		}
		iNodeToKill.write(blockData, 0, Constants.BLOCK_SIZE);
		myDBCache.releaseBlock(iNodeToKill);
	}

    private void overwrite(byte[] original, byte[] data, int offset) {
        for (int i = 0; i < data.length; i++) {
            original[offset + i] = data[i];
        }
    }

	// scan VDF to form the metadata stuctures
	private void initializeMData() {
		formatFreeBlockList(); // set free block list to all free blocks

		for (int i = 1; i <= Constants.NUM_OF_INODES; i++) {
			ArrayList<Integer> iNodeInfo = parseINode(new DFileID(i));
			if (iNodeInfo.get(0) == 0) {
				myFreeINodes.add(i);
			}
            else {
				myDFiles.put(new DFileID(i), new LockState(true, 0));
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
		myDBCache.releaseBlock(blockToParse);
		for (int loc = dFID.offset(); loc < Constants.INODE_SIZE; loc += 4)
			parsedINode.add(ByteBuffer.wrap(
					Arrays.copyOfRange(buffer, loc, loc + 4)).getInt());

		return parsedINode;
	}

    private synchronized boolean acquireReadLock(DFileID dFID)
    {
        LockState state = myDFiles.get(dFID);
        if (state == null) return false;
        while (!state.isShared) {
            try {
                wait();
            }
            catch (InterruptedException e) {}
        }
        state.numReaders++;
        return true;
    }

    private synchronized boolean releaseReadLock(DFileID dFID)
    {
        LockState state = myDFiles.get(dFID);
        if (state == null) return false;
        state.numReaders--;
        return true;
    }

    private synchronized boolean acquireWriteLock(DFileID dFID)
    {
        LockState state = myDFiles.get(dFID);
        if (state == null) return false;

		while (!state.isShared) {  // another thread has the write lock
			try {
				wait();
			}
            catch (InterruptedException e) {}
		}
		state.isShared=false;
		while (state.numReaders > 0) {
			try {
				wait();
			}
            catch (InterruptedException e) {}
		}
        return true;
    }

    private synchronized boolean releaseWriteLock(DFileID dFID)
    {
        LockState state = myDFiles.get(dFID);
        if (state == null) return false;

        state.isShared = true;
        return true;
    }

	private class LockState {
		public boolean isShared;
		public int numReaders;

		public LockState(boolean shared, int readers) {
            isShared=shared;
            numReaders=readers;
		}
	}
}
