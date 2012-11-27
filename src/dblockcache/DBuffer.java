package dblockcache;

import common.Constants.DiskOperationType;


public class DBuffer {
    private int myId;
    private VirtualDisk myVirtualDisk;
    private int myIoOperations;
    private boolean myValid, myClean;


    public DBuffer(int id, VirtualDisk virtualDisk)
    {
        myId = id;
        myVirtualDisk = virtualDisk;
        myIoOperations = 0;
        myValid = false;
        myClean = true;
    }

	/* Start an asynchronous fetch of associated block from the volume */
	public void startFetch()
    {
        myIoOperations++;
        myVirtualDisk.startRequest(this, DiskOperationType.READ);
    }

	/* Start an asynchronous write of buffer contents to block on volume */
	public void startPush()
    {
        myIoOperations++;
        myVirtualDisk.startRequest(this, DiskOperationType.WRITE);
    }

	/* Check whether the buffer has valid data */ 
	public boolean checkValid()
    {
        return myValid;
    }

	/* Wait until the buffer is free */
	public abstract boolean waitValid();

	/* Check whether the buffer is dirty, i.e., has modified data to be written back */
	public boolean checkClean()
    {
        return myClean;
    }

	/* Wait until the buffer is clean, i.e., until a push operation completes */
	public abstract boolean waitClean();

	/* Check if buffer is evictable: not evictable if I/O in progress, or buffer is held */
	public boolean isBusy()
    {
        return myIoOperations > 0;
    }

	/*
	 * reads into the buffer[] array from the contents of the DBuffer. Check
	 * first that the DBuffer has a valid copy of the data! startOffset and
	 * count are for the buffer array, not the DBuffer. Upon an error, it should
	 * return -1, otherwise return number of bytes read.
	 */
	public abstract int read(byte[] buffer, int startOffset, int count);

	/*
	 * writes into the DBuffer from the contents of buffer[] array. startOffset
	 * and count are for the buffer array, not the DBuffer. Mark buffer dirty!
	 * Upon an error, it should return -1, otherwise return number of bytes
	 * written.
	 */
	public abstract int write(byte[] buffer, int startOffset, int count);

	/* An upcall from VirtualDisk layer to inform the completion of an IO operation */
	public abstract void ioComplete();

	/* An upcall from VirtualDisk layer to fetch the blockID associated with a startRequest operation */
	public abstract int getBlockID();

	/* An upcall from VirtualDisk layer to fetch the buffer associated with DBuffer object*/
	public abstract byte[] getBuffer(); 
}