/*
 * Copyright (c) 2013-2015 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package edu.mit.streamjit.impl.compiler2;

import static com.google.common.base.Preconditions.checkState;
import com.google.common.collect.ImmutableSet;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.common.BlobHostStreamCompiler;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.test.Datasets;
import java.nio.file.Path;
import java.util.Random;
import java.util.Set;

/**
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 11/12/2013 (from CompilerStreamCompiler since 8/13/2013)
 */
public final class Compiler2StreamCompiler extends BlobHostStreamCompiler {
	private Configuration config;
	private int randomSeed = -1;
	private int maxNumCores = Compiler2.ALLOCATION_STRATEGY.maxNumCores();
	private int multiplier = 1;
	private Path dumpFile;
	private boolean timings = false;
	private boolean throughput = false;
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

	public Compiler2StreamCompiler reportThroughput() {
		this.throughput = true;
		return this;
	}

	@Override
	protected final int getMaxNumCores() {
		return maxNumCores;
	}

	@Override
	protected final Configuration getConfiguration(Set<Worker<?, ?>> workers) {
		if (config != null) {
			Configuration.Builder builder = Configuration.builder(config);
			builder.putExtraData("reportThroughput", throughput);
			return builder.build();
		}

		Configuration defaultConfiguration = super.getConfiguration(workers);
		if (randomSeed != -1)
			return Configuration.randomize(defaultConfiguration, new Random(randomSeed));

		Configuration.Builder builder = Configuration.builder(defaultConfiguration);
		Configuration.IntParameter multiplierParam = (Configuration.IntParameter)builder.removeParameter("multiplier");
		builder.addParameter(new Configuration.IntParameter("multiplier", multiplierParam.getRange(), this.multiplier));

		if (dumpFile != null)
			builder.putExtraData("dumpFile", dumpFile);
		builder.putExtraData("timings", timings);
		builder.putExtraData("reportThroughput", throughput);
		return builder.build();
	}

	@Override
	protected Blob makeBlob(ImmutableSet<Worker<?, ?>> workers, Configuration configuration, Input<?> input, Output<?> output) {
		//When reporting throughput, repeat the input as needed.
		Boolean reportThroughput = (Boolean)configuration.getExtraData("reportThroughput");
		if (reportThroughput != null && reportThroughput)
			input = Datasets.cycle(input);
		return new Compiler2(workers, configuration, getMaxNumCores(), null, input, output).compile();
	}

	@Override
	protected Buffer makeInputBuffer(Input<?> input, int minCapacity) {
		return null; //handled by Compiler2
	}

	@Override
	protected Buffer makeOutputBuffer(Output<?> output, int minCapacity) {
		return null; //handled by Compiler2
	}

	@Override
	public String toString() {
		return String.format("Compiler2StreamCompiler (%d cores %d mult)", maxNumCores, multiplier);
	}
}
