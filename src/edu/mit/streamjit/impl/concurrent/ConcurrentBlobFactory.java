package edu.mit.streamjit.impl.concurrent;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.MessageConstraint;

/**
 * Manufactures {@link ConcurrentBlob}.
 * @author Sumanan sumanan@mit.edu
 * @since Apr 10, 2013
 */
public class ConcurrentBlobFactory implements BlobFactory {

	@Override
	public Blob makeBlob(Set<Worker<?, ?>> workers, Configuration config, int maxNumCores) {
		return new ConcurrentBlob(workers, config, maxNumCores, Collections.<MessageConstraint> emptyList());
	}

	@Override
	public Configuration getDefaultConfiguration(Set<Worker<?, ?>> workers) {
		return null;
	}
}
