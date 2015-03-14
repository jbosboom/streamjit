/*
 * Copyright (c) 2015 Massachusetts Institute of Technology
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

import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.SwitchParameter;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.util.bytecode.methodhandles.Combinators;
import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.Set;

/**
 * A strategy for multiplexing splitter outputs and joiner inputs through a
 * single method handle.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 3/7/2015
 */
public interface SwitchingStrategy {
	/**
	 * Adds parameters used by this strategy to the given builder.
	 * @param workers the workers the configuration is being built for
	 * @param builder the builder
	 */
	public void makeParameters(Set<Worker<?, ?>> workers, Configuration.Builder builder);

	public MethodHandle createSwitch(MethodHandle[] handles, WorkerActor worker, Configuration config);

	public static SwitchingStrategy tableswitch() {
		class TableswitchStrategy implements SwitchingStrategy {
			@Override
			public void makeParameters(Set<Worker<?, ?>> workers, Configuration.Builder builder) {}
			@Override
			public MethodHandle createSwitch(MethodHandle[] handles, WorkerActor worker, Configuration config) {
				return Combinators.tableswitch(handles);
			}
		}
		return new TableswitchStrategy();
	}

	public static SwitchingStrategy lookupswitch() {
		class LookupswitchStrategy implements SwitchingStrategy {
			@Override
			public void makeParameters(Set<Worker<?, ?>> workers, Configuration.Builder builder) {}
			@Override
			public MethodHandle createSwitch(MethodHandle[] handles, WorkerActor worker, Configuration config) {
				return Combinators.lookupswitch(handles);
			}
		}
		return new LookupswitchStrategy();
	}

	public static SwitchingStrategy tunePerWorker() {
		class LookupswitchStrategy implements SwitchingStrategy {
			@Override
			public void makeParameters(Set<Worker<?, ?>> workers, Configuration.Builder builder) {
				for (Worker<?, ?> w : workers) {
					if (w instanceof Splitter)
						builder.addParameter(new SwitchParameter<>("Switching"+Workers.getIdentifier(w),
								String.class, "lookupswitch", Arrays.asList("lookupswitch", "tableswitch")));
					if (w instanceof Joiner)
						builder.addParameter(new SwitchParameter<>("Switching"+Workers.getIdentifier(w),
								String.class, "lookupswitch", Arrays.asList("lookupswitch", "tableswitch")));
				}
			}
			@Override
			public MethodHandle createSwitch(MethodHandle[] handles, WorkerActor worker, Configuration config) {
				SwitchParameter<String> p = config.getParameter("Switching"+worker.id(), SwitchParameter.class, String.class);
				switch (p.getValue()) {
					case "lookupswitch": return Combinators.lookupswitch(handles);
					case "tableswitch": return Combinators.tableswitch(handles);
					default: throw new AssertionError(p.getValue());
				}
			}
		}
		return new LookupswitchStrategy();
	}
}
