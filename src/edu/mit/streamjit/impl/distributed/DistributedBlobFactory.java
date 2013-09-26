package edu.mit.streamjit.impl.distributed;

import java.util.Set;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.common.Configuration.Parameter;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.compiler.Compiler;
import edu.mit.streamjit.impl.compiler.CompilerBlobFactory;
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

	public DistributedBlobFactory(int noOfMachines) {
		this.noOfMachines = noOfMachines;
	}

	@Override
	public Blob makeBlob(Set<Worker<?, ?>> workers, Configuration config,
			int maxNumCores, DrainData initialState) {
		return new Compiler(workers, config, maxNumCores, initialState)
				.compile();
	}

	@Override
	public Configuration getDefaultConfiguration(Set<Worker<?, ?>> workers) {
		BlobFactory compilerBF = new CompilerBlobFactory();
		Configuration compilercfg = compilerBF.getDefaultConfiguration(workers);
		Configuration.Builder builder = Configuration.builder(compilercfg);
		Configuration.IntParameter multiplierParam = (Configuration.IntParameter) builder
				.removeParameter("multiplier");
		for (Worker<?, ?> w : workers) {
			Parameter p = new Configuration.IntParameter(String.format(
					"worker%dtomachine", Workers.getIdentifier(w)), 1,
					noOfMachines, 1);
			builder.addParameter(p);
		}

		for (int i = 0; i < noOfMachines; i++) {
			builder.addParameter(new Configuration.IntParameter(String.format(
					"multiplier%d", (i + 1)), multiplierParam.getRange(),
					multiplierParam.getValue()));
		}

		// This parameter cannot be tuned. Its added here because we need this
		// parameter to run the app.
		// TODO: Consider using partition parameter and extradata to store this
		// kind of not tunable data.
		IntParameter noOfMachinesParam = new IntParameter("noOfMachines",
				noOfMachines, noOfMachines, noOfMachines);

		builder.addParameter(noOfMachinesParam);

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
