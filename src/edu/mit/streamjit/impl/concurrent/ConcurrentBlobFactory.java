package edu.mit.streamjit.impl.concurrent;

import java.util.Set;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.Parameter;
import edu.mit.streamjit.impl.compiler2.Compiler2BlobFactory;
import edu.mit.streamjit.impl.distributed.ConfigurationManager;
import edu.mit.streamjit.impl.distributed.WorkerMachine;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;

public class ConcurrentBlobFactory implements BlobFactory {

	private int noOfBlobs;

	private final ConfigurationManager cfgManager;

	public ConcurrentBlobFactory(ConfigurationManager cfgManager, int noOfBlobs) {
		this.cfgManager = cfgManager;
		this.noOfBlobs = noOfBlobs;
	}

	/**
	 * If {@link ConfigurationManager} is not passed as a constructor argument
	 * then {@link WorkerMachine} will be used as default one.
	 * 
	 * @param noOfMachines
	 */
	public ConcurrentBlobFactory(int noOfBlobs) {
		this(new WorkerMachine(null), noOfBlobs);
	}

	@Override
	public Blob makeBlob(Set<Worker<?, ?>> workers, Configuration config,
			int maxNumCores, DrainData initialState) {
		return new Compiler2BlobFactory().makeBlob(workers, config,
				maxNumCores, initialState);
	}

	@Override
	public Configuration getDefaultConfiguration(Set<Worker<?, ?>> workers) {
		Configuration concurrentCfg;
		if (this.noOfBlobs > 1)
			concurrentCfg = cfgManager.getDefaultConfiguration(workers,
					noOfBlobs);
		else
			concurrentCfg = Configuration.builder().build();

		if (!GlobalConstants.useCompilerBlob)
			return concurrentCfg;

		Configuration.Builder builder = Configuration.builder(concurrentCfg);
		BlobFactory compilerBf = new Compiler2BlobFactory();
		Configuration compilercfg = compilerBf.getDefaultConfiguration(workers);
		for (Parameter p : compilercfg.getParametersMap().values())
			builder.addParameter(p);
		return builder.build();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((cfgManager == null) ? 0 : cfgManager.hashCode());
		result = prime * result + noOfBlobs;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ConcurrentBlobFactory other = (ConcurrentBlobFactory) obj;
		if (cfgManager == null) {
			if (other.cfgManager != null)
				return false;
		} else if (!cfgManager.equals(other.cfgManager))
			return false;
		if (noOfBlobs != other.noOfBlobs)
			return false;
		return true;
	}
}
