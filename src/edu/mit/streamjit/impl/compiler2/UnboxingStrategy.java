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
package edu.mit.streamjit.impl.compiler2;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import java.util.Set;

/**
 * A strategy for unboxing. Such a strategy involves making parameters and
 * interpreting them.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 1/15/2014
 */
public interface UnboxingStrategy {
	/**
	 * Adds parameters used by this strategy to the given builder.
	 * @param workers the workers the configuration is being built for
	 * @param builder the builder
	 */
	public void makeParameters(Set<Worker<?, ?>> workers, Configuration.Builder builder);

	/**
	 * Returns whether this storage should be unboxed.
	 * @param storage the storage to (maybe) unbox
	 * @param config config the configuration (will contain parameters created in
	 * makeParameters)
	 * @return whether the storage should be unboxed.
	 */
	public boolean unboxStorage(Storage storage, Configuration config);

	/**
	 * Returns whether this actor's input should be unboxed.
	 * @param actor the actor to (maybe) unbox the input of
	 * @param config config the configuration (will contain parameters created in
	 * makeParameters)
	 * @return whether the actor's input should be unboxed.
	 */
	public boolean unboxInput(WorkerActor actor, Configuration config);

	/**
	 * Returns whether this actor's output should be unboxed.
	 * @param actor the actor to (maybe) unbox the output of
	 * @param config config the configuration (will contain parameters created in
	 * makeParameters)
	 * @return whether the actor's output should be unboxed.
	 */
	public boolean unboxOutput(WorkerActor actor, Configuration config);
}
