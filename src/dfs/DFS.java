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
        allocateBlocks(dFID, 0);
        releaseWriteLock(newFile);
		return newFile;
	}

	/* destroys the file specified by the DFileID */
	public void destroyDFile(DFileID dFID) {
		ArrayList<Integer> iNodeInfo = parseINode(dFID);

		formatBlock(dFID.id());
		myFreeINodes.remove(dFID);
        acquireWriteLock(dFID);
		myDFiles.remove(dFID);
        releaseWriteLock(dFID);
		for (int i = 1; i < iNodeInfo.size(); i++) {
			myFreeBlocks.remove(iNodeInfo.get(i));
			formatBlock(iNodeInfo.get(i));
		}
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

        acquireWriteLock(dFID);
        List<Integer> iNodeInfo = allocateBlocks(dFID, count);
        int bytesWritten = 0;
        for (int i = 1; i <= iNodeInfo.get(0); i++) {
            DBuffer block = myDBCache.getBlock(iNodeInfo.get(i));
            bytesWritten += block.write(buffer, startOffset + bytesWritten,
                                        count - bytesWritten);
			myDBCache.releaseBlock(block);
        }
        releaseWriteLock(dFID);
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
	private void formatBlock(int dFID) {
		byte[] zeroBuffer = new byte[Constants.BLOCK_SIZE];
		for (byte b : zeroBuffer) {
			zeroBuffer[b] = 0;
		}
		DBuffer iNodeToKill = myDBCache.getBlock(dFID);
		iNodeToKill.write(zeroBuffer, 0, Constants.BLOCK_SIZE);
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
		DBuffer blockToParse = myDBCache.getBlock(dFID.block())
		blockToParse.read(buffer, 0, Constants.BLOCK_SIZE);
		myDBCache.releaseBlock(blockToParse);
		for (int loc = dFID.offset(); loc < Constants.INODE_SIZE; loc += 4)
			parsedINode.add(ByteBuffer.wrap(
					Arrays.copyOfRange(buffer, loc, loc + 4)).getInt());

		return parsedINode;
	}

	private void writeINode(DFileID dFID, List<Integer> iNodeInfo) {
		ByteBuffer b = ByteBuffer.allocate(Constants.INODE_SIZE);
        int numBlocks = iNodeInfo.get(0);
        b.putInt(numBlocks);
        for (int i = 1; i <= numBlocks; i++) {
            b.putInt(iNodeInfo.get(i));
        }
		byte[] newINode = b.array();

		byte[] buffer = new byte[Constants.BLOCK_SIZE];
        DBuffer inode = myDBCache.getBlock(dFID.block());
		inode.read(buffer, 0, Constants.BLOCK_SIZE);
        overwrite(buffer, newINode, dFID.offset());
		container.write(buffer, 0, Constants.BLOCK_SIZE);
		myDBCache.releaseBlock(container);
	}


    private synchronized List<Integer> allocateBlocks(DFileID dFID, int count) {
		ArrayList<Integer> iNodeInfo = parseINode(dFID);
        int currentBlocks = iNodeInfo.get(0);
        int numBlocks = count / Constants.BLOCK_SIZE +
            (count % Constants.BLOCK_SIZE == 0 ? 0 : 1);
        numBlocks = Math.max(1, numBlocks);

        if (numBlocks < currentBlocks) {
            for (int i = currentBlocks - numBlocks; i < currentBlocks; i++) {
                myFreeBlocks.add(iNodeInfo.get(i));
            }
        }
        if (numBlocks > currentBlocks) {
            for (int i = numBlocks - currentBlocks; i < numBlocks; i++) {
                Integer newBlock = myFreeBlocks.first();
                myFreeBlocks.remove(newBlock);
                iNodeInfo.add(newBlock);
            }
        }
        if (numBlocks != currentBlocks) {
            writeINode(dFID, iNodeInfo);
        }
        return iNodeInfo;
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
