package edu.mit.streamjit.impl.compiler2;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.IOInfo;
import edu.mit.streamjit.impl.common.Workers;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * TODO: describe the restrictions on the worker set
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 9/22/2013 (from CompilerBlobFactory since 4/24/2013)
 */
public final class Compiler2BlobFactory implements BlobFactory {
	public Compiler2BlobFactory() {}

	@Override
	public Blob makeBlob(Set<Worker<?, ?>> workers, Configuration config, int maxNumCores, DrainData initialState) {
		return new Compiler2(workers, config, maxNumCores, initialState).compile();
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
			if (!Workers.isPeeking(w))
				builder.addParameter(Configuration.SwitchParameter.create("fuse"+Workers.getIdentifier(w), true));
		//One IntParameter for each worker (possibly they're all separate nodes)
		//and each core to determine how many multiples to put on that core.
		for (Worker<?, ?> w : workers)
			for (int i = 0; i < MAX_MAX_NUM_CORES; ++i) {
				String name = String.format("node%dcore%diter", Workers.getIdentifier(w), i);
				builder.addParameter(new Configuration.IntParameter(name, 0, 1_000_000, 1));
			}
		for (Worker<?, ?> w : workers)
			for (int i = 0; i < MAX_MAX_NUM_CORES; ++i) {
				List<String> names = new ArrayList<>();
				for (int j = 0; j < w.getPopRates().size(); ++j)
					names.add(String.format("Core%dWorker%dInput%dIndexFxnTransformer", i, Workers.getIdentifier(w), j));
				for (int j = 0; j < w.getPushRates().size(); ++j)
					names.add(String.format("Core%dWorker%dOutput%dIndexFxnTransformer", i, Workers.getIdentifier(w), j));
				for (String name : names)
					builder.addParameter(new Configuration.SwitchParameter<>(name, IndexFunctionTransformer.class,
							Compiler2.INDEX_FUNCTION_TRANSFORMERS.asList().get(0),
							Compiler2.INDEX_FUNCTION_TRANSFORMERS));
			}
		for (Worker<?, ?> w : workers)
			if (Compiler2.REMOVABLE_WORKERS.contains(w.getClass()))
				builder.addParameter(Configuration.SwitchParameter.create("remove"+Workers.getIdentifier(w), true));
		for (IOInfo i : IOInfo.allEdges(workers))
			builder.addParameter(Configuration.SwitchParameter.create("unboxStorage"+i.token(), i.isInternal()));
		for (Worker<?, ?> w : workers) {
			builder.addParameter(Configuration.SwitchParameter.create("unboxInput"+Workers.getIdentifier(w), true));
			builder.addParameter(Configuration.SwitchParameter.create("unboxOutput"+Workers.getIdentifier(w), true));
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
