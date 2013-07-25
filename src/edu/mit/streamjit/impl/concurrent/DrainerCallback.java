package edu.mit.streamjit.impl.concurrent;

import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.common.BlobThread;

/**
 * Each {@link Blob} passes this runnable object to next {@link Blob}.drain() to
 * synchronize the draining operation.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Apr 10, 2013
 */
public class DrainerCallback implements Runnable {

	private List<Blob> blobList;
	private Map<Blob, Set<BlobThread>> threads;
	private volatile int currentBlob;
	private volatile boolean isDrained;

	public DrainerCallback(List<Blob> blobList,
			Map<Blob, Set<BlobThread>> threads) {
		this.blobList = blobList;
		this.threads = threads;
		currentBlob = 0;
		isDrained = false;
	}

	public void setBlobList(List<Blob> blobList) {
		this.blobList = blobList;
		currentBlob = 0;
	}

	@Override
	public void run() {
		// Stop current blob's threads.
		for (BlobThread t : threads.get(blobList.get(currentBlob)))
			t.requestStop();
		++currentBlob;

		if (currentBlob < blobList.size()) {
			blobList.get(currentBlob).drain(this);
		} else {
			isDrained = true;
			System.out.println("DrainerCallback: Drainig completed");
		}
	}

	public boolean isDrained() {
		return isDrained;
	}
}