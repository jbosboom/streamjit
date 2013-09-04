package edu.mit.streamjit.impl.compiler;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.BlobHostStreamCompiler;
import edu.mit.streamjit.impl.common.Configuration;
import java.nio.file.Path;
import java.util.Set;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/13/2013
 */
public final class CompilerStreamCompiler extends BlobHostStreamCompiler {
	private int maxNumCores = 1;
	private int multiplier = 1;
	private Path dumpFile;
	public CompilerStreamCompiler() {
		super(new CompilerBlobFactory());
	}

	public CompilerStreamCompiler maxNumCores(int maxNumCores) {
		this.maxNumCores = maxNumCores;
		return this;
	}

	public CompilerStreamCompiler multiplier(int multiplier) {
		this.multiplier = multiplier;
		return this;
	}

	public CompilerStreamCompiler dumpFile(Path path) {
		this.dumpFile = path;
		return this;
	}

	@Override
	protected final int getMaxNumCores() {
		return maxNumCores;
	}

	@Override
	protected final Configuration getConfiguration(Set<Worker<?, ?>> workers) {
		Configuration.Builder builder = Configuration.builder(super.getConfiguration(workers));
		Configuration.IntParameter multiplierParam = (Configuration.IntParameter)builder.removeParameter("multiplier");
		builder.addParameter(new Configuration.IntParameter("multiplier", multiplierParam.getRange(), this.multiplier));
		if (dumpFile != null)
			builder.putExtraData("dumpFile", dumpFile);
		return builder.build();
	}

	@Override
	public String toString() {
		return String.format("CompilerStreamCompiler (%d cores %d mult)", maxNumCores, multiplier);
	}
}
