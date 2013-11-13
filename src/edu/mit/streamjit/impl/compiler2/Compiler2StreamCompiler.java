package edu.mit.streamjit.impl.compiler2;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.BlobHostStreamCompiler;
import edu.mit.streamjit.impl.common.Configuration;
import java.nio.file.Path;
import java.util.Set;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/12/2013 (from CompilerStreamCompiler since 8/13/2013)
 */
public final class Compiler2StreamCompiler extends BlobHostStreamCompiler {
	private int maxNumCores = 1;
	private int multiplier = 1;
	private Path dumpFile;
	public Compiler2StreamCompiler() {
		super(new Compiler2BlobFactory());
	}

	public Compiler2StreamCompiler maxNumCores(int maxNumCores) {
		this.maxNumCores = maxNumCores;
		return this;
	}

	public Compiler2StreamCompiler multiplier(int multiplier) {
		this.multiplier = multiplier;
		return this;
	}

	public Compiler2StreamCompiler dumpFile(Path path) {
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
