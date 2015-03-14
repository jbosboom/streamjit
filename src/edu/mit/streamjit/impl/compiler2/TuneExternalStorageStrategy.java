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

import com.google.common.collect.ImmutableList;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.IOInfo;
import java.util.Set;

/**
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 3/1/2014
 */
public final class TuneExternalStorageStrategy implements StorageStrategy {
	private final ImmutableList<Arrayish.Factory> ARRAYISH_FACTORIES = ImmutableList.of(
			Arrayish.ArrayArrayish.factory(),
			Arrayish.NIOArrayish.factory(),
			Arrayish.UnsafeArrayish.factory()
	);
	@Override
	public void makeParameters(Set<Worker<?, ?>> workers, Configuration.Builder builder) {
		for (IOInfo i : IOInfo.allEdges(workers)) {
			builder.addParameter(new Configuration.SwitchParameter<>("ExternalArrayish"+i.token(), Arrayish.Factory.class, ARRAYISH_FACTORIES.get(0), ARRAYISH_FACTORIES));
			builder.addParameter(Configuration.SwitchParameter.create("UseDoubleBuffers"+i.token(), true));
		}
	}
	@Override
	public StorageFactory asFactory(final Configuration config) {
		return (Storage storage) -> {
			if (storage.steadyStateCapacity() == 0)
				return new EmptyConcreteStorage(storage);

			Configuration.SwitchParameter<Arrayish.Factory> factoryParam = config.getParameter("ExternalArrayish"+storage.id(), Configuration.SwitchParameter.class, Arrayish.Factory.class);
			Arrayish.Factory factory = storage.type().isPrimitive() ? factoryParam.getValue() : Arrayish.ArrayArrayish.factory();
			Configuration.SwitchParameter<Boolean> useDoubleBuffersParam = config.getParameter("UseDoubleBuffers"+storage.id(), Configuration.SwitchParameter.class, Boolean.class);
			if (useDoubleBuffersParam.getValue()
					&& storage.steadyStateCapacity() == 2*storage.throughput() //no leftover data
					&& storage.isFullyExternal() //no reads of writes before adjust
					)
				return new DoubleArrayConcreteStorage(factory, storage);
			return new CircularArrayConcreteStorage(factory.make(storage.type(), storage.steadyStateCapacity()), storage);
		};
	}
}
