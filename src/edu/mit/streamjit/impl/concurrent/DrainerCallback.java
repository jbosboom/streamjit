package edu.mit.streamjit.impl.concurrent;

import java.util.List;

import edu.mit.streamjit.impl.blob.Blob;

/**
 * Each {@link Blob} passes this runnable object to next {@link Blob}.drain() to
 * synchronize the draining operation.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Apr 10, 2013
 */
public class DrainerCallback implements Runnable {

	private List<Blob> blobList;
	private volatile int currentBlob;

	public DrainerCallback(List<Blob> blobList) {
		this.blobList = blobList;
		currentBlob = 0;
	}

	public void setBlobList(List<Blob> blobList) {
		this.blobList = blobList;
		currentBlob = 0;
	}

	@Override
	public void run() {
		// System.out.println("I am drainer callback. I am called by " +
		// Thread.currentThread().getName());
		if (currentBlob < blobList.size())
			blobList.get(currentBlob++).drain(this);
		else
			System.out.println("DrainerCallback: Drainig completed");
	}
}