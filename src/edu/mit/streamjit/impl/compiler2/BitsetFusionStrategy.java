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

import edu.mit.streamjit.api.StatefulFilter;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Workers;
import java.util.Set;

/**
 * A fusion strategy using a set of boolean parameters.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 1/15/2014
 */
public final class BitsetFusionStrategy implements FusionStrategy {
	@Override
	public void makeParameters(Set<Worker<?, ?>> workers, Configuration.Builder builder) {
		for (Worker<?, ?> w : workers)
			if (!Workers.isPeeking(w))
				builder.addParameter(Configuration.SwitchParameter.create("fuse"+Workers.getIdentifier(w), !(w instanceof StatefulFilter)));
	}

	@Override
	public boolean fuseUpward(ActorGroup group, Configuration config) {
		String paramName = String.format("fuse%d", group.id());
		Configuration.SwitchParameter<Boolean> param = config.getParameter(paramName, Configuration.SwitchParameter.class, Boolean.class);
		return param.getValue();
	}
}
