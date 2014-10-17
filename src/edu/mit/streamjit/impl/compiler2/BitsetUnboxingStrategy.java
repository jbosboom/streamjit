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
import edu.mit.streamjit.impl.common.IOInfo;
import edu.mit.streamjit.impl.common.Workers;
import java.util.Set;

/**
 * An unboxing strategy using a set of boolean parameters.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 1/15/2014
 */
public final class BitsetUnboxingStrategy implements UnboxingStrategy {
	@Override
	public void makeParameters(Set<Worker<?, ?>> workers, Configuration.Builder builder) {
		for (IOInfo i : IOInfo.allEdges(workers))
			builder.addParameter(Configuration.SwitchParameter.create("unboxStorage"+i.token().toString().replace(", ", "_"), i.isInternal()));
		for (Worker<?, ?> w : workers) {
			builder.addParameter(Configuration.SwitchParameter.create("unboxInput"+Workers.getIdentifier(w), true));
			builder.addParameter(Configuration.SwitchParameter.create("unboxOutput"+Workers.getIdentifier(w), true));
		}
	}

	@Override
	public boolean unboxStorage(Storage storage, Configuration config) {
		Configuration.SwitchParameter<Boolean> param = config.getParameter("unboxStorage"+storage.id().toString().replace(", ", "_"), Configuration.SwitchParameter.class, Boolean.class);
		return param.getValue();
	}

	@Override
	public boolean unboxInput(WorkerActor actor, Configuration config) {
		Configuration.SwitchParameter<Boolean> param = config.getParameter("unboxInput"+actor.id(), Configuration.SwitchParameter.class, Boolean.class);
		return param.getValue();
	}

	@Override
	public boolean unboxOutput(WorkerActor actor, Configuration config) {
		Configuration.SwitchParameter<Boolean> param = config.getParameter("unboxOutput"+actor.id(), Configuration.SwitchParameter.class, Boolean.class);
		return param.getValue();
	}
}
