/**
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
package edu.mit.streamjit.impl.distributed.runtime.slave;

import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.distributed.runtime.api.BlobExecuter;
import edu.mit.streamjit.impl.distributed.runtime.api.BlobsManager;

public class BlobsManagerImpl implements BlobsManager {
	ImmutableSet<Blob> blobSet;
	
	BlobsManagerImpl(ImmutableSet<Blob> blobSet) {
		this.blobSet = blobSet;
	}

	@Override
	public void start() {
		// TODO Auto-generated method stub

	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub

	}

	@Override
	public void suspend() {
		// TODO Auto-generated method stub

	}

	@Override
	public void resume() {
		// TODO Auto-generated method stub

	}

}
