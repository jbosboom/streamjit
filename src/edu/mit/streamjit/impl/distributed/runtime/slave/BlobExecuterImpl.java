/**
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
package edu.mit.streamjit.impl.distributed.runtime.slave;

import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.distributed.runtime.api.BlobExecuter;

public class BlobExecuterImpl implements BlobExecuter {
	
	Blob blob;
	
	public BlobExecuterImpl(Blob blob)
	{
		this.blob = blob;
	}

	@Override
	public void start() {	
		
	}

	@Override
	public void stop() {
	}

	@Override
	public void suspend() {
	}

	@Override
	public void resume() {
	}
}
