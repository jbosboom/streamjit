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
package edu.mit.streamjit.test;

import edu.mit.streamjit.util.ConstructorSupplier;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import edu.mit.streamjit.api.OneToOneElement;

/**
 * A Benchmark implementation that instantiates a stream graph from a Supplier
 * instance. Also includes convenience constructors for common Supplier
 * implementations, such as constructors.
 * <p/>
 * This class is nonfinal to allow subclasses to specify constructor arguments.
 * The subclasses then have a no-arg constructor required for the ServiceLoader
 * mechanism. The Benchmark interface methods are final.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 8/13/2013
 */
public class SuppliedBenchmark extends AbstractBenchmark {
	private final Supplier<? extends OneToOneElement> supplier;
	public SuppliedBenchmark(String name, Supplier<? extends OneToOneElement> supplier, Dataset firstInput, Dataset... moreInputs) {
		super(name, firstInput, moreInputs);
		this.supplier = supplier;
	}
	public <T> SuppliedBenchmark(String name, Class<? extends OneToOneElement> streamClass, Iterable<?> arguments, Dataset firstInput, Dataset... moreInputs) {
		this(name, new ConstructorSupplier<>(streamClass, arguments), firstInput, moreInputs);
	}
	public SuppliedBenchmark(String name, Class<? extends OneToOneElement> streamClass, Dataset firstInput, Dataset... moreInputs) {
		this(name, streamClass, ImmutableList.of(), firstInput, moreInputs);
	}

	@Override
	@SuppressWarnings("unchecked")
	public final OneToOneElement<Object, Object> instantiate() {
		return supplier.get();
	}
}
