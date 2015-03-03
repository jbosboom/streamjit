package edu.mit.streamjit.impl.compiler2;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.util.affinity.Affinity;

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
		return new Compiler2(workers, config, maxNumCores, initialState, null, null).compile();
	}

	@Override
	public Configuration getDefaultConfiguration(Set<Worker<?, ?>> workers) {
		Configuration.Builder builder = Configuration.builder();
		Compiler2.REMOVAL_STRATEGY.makeParameters(workers, builder);
		Compiler2.FUSION_STRATEGY.makeParameters(workers, builder);
		Compiler2.UNBOXING_STRATEGY.makeParameters(workers, builder);
		Compiler2.ALLOCATION_STRATEGY.makeParameters(workers, builder);
		Compiler2.INTERNAL_STORAGE_STRATEGY.makeParameters(workers, builder);
		Compiler2.EXTERNAL_STORAGE_STRATEGY.makeParameters(workers, builder);
		for (Worker<?, ?> w : workers)
			for (int i = 0; i < Compiler2.ALLOCATION_STRATEGY.maxNumCores(); ++i) {
				int id = Workers.getIdentifier(w);
//				List<String> names = new ArrayList<>();
//				for (int j = 0; j < w.getPopRates().size(); ++j)
//					names.add(String.format("Core%dWorker%dInput%dIndexFxnTransformer", i, id, j));
//				for (int j = 0; j < w.getPushRates().size(); ++j)
//					names.add(String.format("Core%dWorker%dOutput%dIndexFxnTransformer", i, id, j));
//				for (String name : names)
//					builder.addParameter(new Configuration.SwitchParameter<>(name, IndexFunctionTransformer.class,
//							Compiler2.INDEX_FUNCTION_TRANSFORMERS.asList().get(0),
//							Compiler2.INDEX_FUNCTION_TRANSFORMERS));

				builder.addParameter(new Configuration.IntParameter(String.format("UnrollCore%dGroup%d", i, id),
						1, 1024, 1));
			}
		builder.addParameter(Configuration.SwitchParameter.create("UsePeekableBuffer", true));
		//Init scheduling trades off between firings during the init schedule
		//and resulting extra buffering.  My ILP solver interface only supports
		//int coefficients so this is discretized in units of 100.
		builder.addParameter(new Configuration.IntParameter("InitBufferingCost", 0, 100, 100));
		//TODO: this really belongs in BlobHostStreamCompiler, but we have to
		//add it here or we won't pick it up in the default configuration.
//		Configuration.PermutationParameter<Integer> affinity = new Configuration.PermutationParameter<>("$affinity", Integer.class, Affinity.getMaximalAffinity());
//		builder.addParameter(affinity);
		return builder.addParameter(new Configuration.IntParameter("multiplier", 1, Options.multiplierMaxValue, 1))
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
