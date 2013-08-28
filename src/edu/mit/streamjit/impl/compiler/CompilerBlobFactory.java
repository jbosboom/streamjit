package edu.mit.streamjit.impl.compiler;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Workers;
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
		Configuration.Builder builder = Configuration.builder();
		for (Worker<?, ?> w : workers)
			builder.addParameter(Configuration.SwitchParameter.create("fuse"+Workers.getIdentifier(w), true));
		return builder.addParameter(new Configuration.IntParameter("multiplier", 1, 1000000, 1))
				.build();
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
