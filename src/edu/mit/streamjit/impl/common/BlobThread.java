package edu.mit.streamjit.impl.common;

import edu.mit.streamjit.impl.blob.Blob;

/**
 * Runner thread to run a core code of a {@link Blob}.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Jul 25, 2013
 */
public final class BlobThread extends Thread {
	private volatile boolean stopping = false;
	private final Runnable coreCode;

	public BlobThread(Runnable coreCode, String name) {
		super(name);
		this.coreCode = coreCode;
	}

	public BlobThread(Runnable coreCode) {
		this.coreCode = coreCode;
	}

	@Override
	public void run() {
		while (!stopping)
			coreCode.run();
	}

	public void requestStop() {
		stopping = true;
	}
}