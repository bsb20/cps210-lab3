package virtualdisk;
import java.io.IOException;

import common.Constants.DiskOperationType;
import dblockcache.DBuffer;


public interface IVirtualDisk {
	
	/*
	 * Start an asynchronous request to the underlying device/disk/volume.   
	 *  -- buf is an DBuffer object that needs to be read/write from/to the volume
	 *  -- operation is either READ or WRITE
	 */
	public void startRequest(DBuffer buf, DiskOperationType operation)
			throws IllegalArgumentException, IOException;
}