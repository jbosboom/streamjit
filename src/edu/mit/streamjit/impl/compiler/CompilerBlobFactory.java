package edu.mit.streamjit.impl.compiler;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.blob.DrainData;
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
	public Blob makeBlob(Set<Worker<?, ?>> workers, Configuration config, int maxNumCores, DrainData initialState) {
		return new Compiler(workers, config, maxNumCores, initialState).compile();
	}

	/**
	 * The maximum number of cores we support data-parallelizing over.  If
	 * maxNumCores is larger than this, we ignore the rest.  Theoretically, the
	 * autotuner should learn to ignore any unused core variables, but we have
	 * a limit anyway.
	 */
	private static final int MAX_MAX_NUM_CORES = 8;
	@Override
	public Configuration getDefaultConfiguration(Set<Worker<?, ?>> workers) {
		Configuration.Builder builder = Configuration.builder();
		for (Worker<?, ?> w : workers)
			builder.addParameter(Configuration.SwitchParameter.create("fuse"+Workers.getIdentifier(w), true));
		//One IntParameter for each worker (possibly they're all separate nodes)
		//and each core to determine how many multiples to put on that core.
		for (Worker<?, ?> w : workers)
			for (int i = 0; i < MAX_MAX_NUM_CORES; ++i) {
				String name = String.format("node%dcore%diter", Workers.getIdentifier(w), i);
				builder.addParameter(new Configuration.IntParameter(name, 0, 1_000_000, 1));
			}
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
