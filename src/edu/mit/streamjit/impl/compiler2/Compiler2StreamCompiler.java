package edu.mit.streamjit.impl.compiler2;

import static com.google.common.base.Preconditions.checkState;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.BlobHostStreamCompiler;
import edu.mit.streamjit.impl.common.Configuration;
import java.nio.file.Path;
import java.util.Random;
import java.util.Set;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/12/2013 (from CompilerStreamCompiler since 8/13/2013)
 */
public final class Compiler2StreamCompiler extends BlobHostStreamCompiler {
	private Configuration config;
	private int randomSeed = -1;
	private int maxNumCores = Compiler2.ALLOCATION_STRATEGY.maxNumCores();
	private int multiplier = 1;
	private Path dumpFile;
	private boolean timings = false;
	public Compiler2StreamCompiler() {
		super(new Compiler2BlobFactory());
	}

	public Compiler2StreamCompiler configuration(Configuration config) {
		this.config = config;
		return this;
	}

	public Compiler2StreamCompiler randomize(int randomSeed) {
		this.randomSeed = randomSeed;
		return this;
	}

	public Compiler2StreamCompiler maxNumCores(int maxNumCores) {
		checkState(config == null, "can't specify when using a specific configuration");
		this.maxNumCores = maxNumCores;
		return this;
	}

	public Compiler2StreamCompiler multiplier(int multiplier) {
		checkState(config == null, "can't specify when using a specific configuration");
		this.multiplier = multiplier;
		return this;
	}

	public Compiler2StreamCompiler dumpFile(Path path) {
		this.dumpFile = path;
		return this;
	}

	public Compiler2StreamCompiler timings() {
		this.timings = true;
		return this;
	}

	@Override
	protected final int getMaxNumCores() {
		return maxNumCores;
	}

	@Override
	protected final Configuration getConfiguration(Set<Worker<?, ?>> workers) {
		if (config != null)
			return config;
		Configuration defaultConfiguration = super.getConfiguration(workers);
		if (randomSeed != -1)
			return Configuration.randomize(defaultConfiguration, new Random(randomSeed));

		Configuration.Builder builder = Configuration.builder(defaultConfiguration);
		Configuration.IntParameter multiplierParam = (Configuration.IntParameter)builder.removeParameter("multiplier");
		builder.addParameter(new Configuration.IntParameter("multiplier", multiplierParam.getRange(), this.multiplier));

		if (dumpFile != null)
			builder.putExtraData("dumpFile", dumpFile);
		builder.putExtraData("timings", timings);
		return builder.build();
	}

	@Override
	public String toString() {
		return String.format("Compiler2StreamCompiler (%d cores %d mult)", maxNumCores, multiplier);
	}
}
