package edu.mit.streamjit.impl.concurrent;

import java.util.List;

import edu.mit.streamjit.impl.blob.Blob;
import java.util.Map;
import java.util.Set;

/**
 * Each {@link Blob} passes this runnable object to next {@link Blob}.drain() to
 * synchronize the draining operation.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Apr 10, 2013
 */
public class DrainerCallback implements Runnable {

	private List<Blob> blobList;
	private Map<Blob, Set<ConcurrentStreamCompiler.ConcurrentCompiledStream.MyThread>> threads;
	private volatile int currentBlob;
	private volatile boolean isDrained;

	public DrainerCallback(
			List<Blob> blobList,
			Map<Blob, Set<ConcurrentStreamCompiler.ConcurrentCompiledStream.MyThread>> threads) {
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
		// System.out.println("I am drainer callback. I am called by " +
		// Thread.currentThread().getName());

		// Stop current blob's threads.
		for (ConcurrentStreamCompiler.ConcurrentCompiledStream.MyThread t : threads
				.get(blobList.get(currentBlob)))
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