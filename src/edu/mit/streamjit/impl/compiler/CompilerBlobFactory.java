package edu.mit.streamjit.impl.compiler;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.common.Configuration;
import java.util.Set;

/**
 * TODO: describe the restrictions on the worker set
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/24/2013
 */
public final class CompilerBlobFactory implements BlobFactory {
	public CompilerBlobFactory() {}

	@Override
	public Blob makeBlob(Set<Worker<?, ?>> workers, Configuration config, int maxNumCores) {
		return new Compiler(workers, config, maxNumCores).compile();
	}

	@Override
	public Configuration getDefaultConfiguration(Set<Worker<?, ?>> workers) {
		throw new UnsupportedOperationException("TODO");
	}

	@Override
	public boolean equals(Object o) {
		return getClass() == o.getClass();
	}

	@Override
	public int hashCode() {
		return 0;
	}
}
