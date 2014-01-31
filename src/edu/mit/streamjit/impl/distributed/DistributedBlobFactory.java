package edu.mit.streamjit.impl.distributed;

import java.util.Set;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.Parameter;
import edu.mit.streamjit.impl.compiler2.Compiler2BlobFactory;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.interp.Interpreter.InterpreterBlobFactory;

/**
 * This BlobFactory is not for any specific blob implementation. Internally, it
 * uses {@link CompilerBlobFactory} and {@link InterpreterBlobFactory} to get
 * default configurations for a given stream graph. In addition to that, it adds
 * all parameters, such as worker to machine assignment, multiplication factor
 * for each machine and etc, which are related to distributed tuning.
 * <p>
 * This {@link BlobFactory} becomes statefull because of the numbers of the
 * machine connected.
 * </p>
 * <p>
 * TODO: For the moment this factory just deal with compiler blob. Need to make
 * interpreter blob as well based on the dynamic edges.
 *
 * @author Sumanan sumanan@mit.edu
 * @since Sep 24, 2013
 */
public class DistributedBlobFactory implements BlobFactory {

	private int noOfMachines;

	private final ConfigurationManager cfgManager;

	public DistributedBlobFactory(ConfigurationManager cfgManager,
			int noOfMachines) {
		this.cfgManager = cfgManager;
		this.noOfMachines = noOfMachines;
	}

	/**
	 * If {@link ConfigurationManager} is not passed as a constructor argument
	 * then {@link WorkerMachine} will be used as default one.
	 *
	 * @param noOfMachines
	 */
	public DistributedBlobFactory(int noOfMachines) {
		this(new WorkerMachine(null), noOfMachines);
	}

	@Override
	public Blob makeBlob(Set<Worker<?, ?>> workers, Configuration config,
			int maxNumCores, DrainData initialState) {
		return new Compiler2BlobFactory().makeBlob(workers, config, maxNumCores, initialState);
	}

	@Override
	public Configuration getDefaultConfiguration(Set<Worker<?, ?>> workers) {

		Configuration distCfg = cfgManager.getDefaultConfiguration(workers,
				noOfMachines);
		if (!GlobalConstants.useCompilerBlob)
			return distCfg;

		Configuration.Builder builder = Configuration.builder(distCfg);
		BlobFactory compilerBf = new Compiler2BlobFactory();
		Configuration compilercfg = compilerBf.getDefaultConfiguration(workers);
		for (Parameter p : compilercfg.getParametersMap().values())
			builder.addParameter(p);
		return builder.build();
	}

	@Override
	public boolean equals(Object o) {
		return getClass() == o.getClass()
				&& noOfMachines == ((DistributedBlobFactory) o).noOfMachines;
	}

	@Override
	public int hashCode() {
		return 31 * noOfMachines;
	}
}
