/*
 * Copyright (c) 2013-2014 Massachusetts Institute of Technology
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
package edu.mit.streamjit.impl.compiler;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.BlobHostStreamCompiler;
import edu.mit.streamjit.impl.common.Configuration;

/**
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
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


	Configuration cfg;

	public void setConfig(Configuration config)
	{
		this.cfg = config;
	}

	@Override
	protected final Configuration getConfiguration(Set<Worker<?, ?>> workers) {
	if(cfg == null){		
		Configuration defaultConfiguration = super.getConfiguration(workers);
		Configuration.Builder builder = Configuration.builder(defaultConfiguration);
		Configuration.IntParameter multiplierParam = (Configuration.IntParameter)builder.removeParameter("multiplier");
		builder.addParameter(new Configuration.IntParameter("multiplier", multiplierParam.getRange(), this.multiplier));

		//For testing, try full data-parallelization across all cores.
		int perCore = multiplier/maxNumCores;
		for (Map.Entry<String, Configuration.Parameter> e : defaultConfiguration.getParametersMap().entrySet())
			if (e.getKey().matches("node(\\d+)core(\\d+)iter")) {
				Configuration.IntParameter p = (Configuration.IntParameter)builder.removeParameter(e.getKey());
				builder.addParameter(new Configuration.IntParameter(e.getKey(), p.getRange(), perCore));
			}

		if (dumpFile != null)
			builder.putExtraData("dumpFile", dumpFile);
		return builder.build();
		}
		return cfg;
	}

	@Override
	public String toString() {
		return String.format("CompilerStreamCompiler (%d cores %d mult)", maxNumCores, multiplier);
	}
}
