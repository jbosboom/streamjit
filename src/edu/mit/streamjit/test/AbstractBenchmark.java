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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;

/**
 * A skeletal Benchmark implementation that manages the name and inputs but
 * leaves instantiate() abstract.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 8/27/2013
 */
public abstract class AbstractBenchmark implements Benchmark {
	private final String name;
	private final ImmutableList<Dataset> inputs;
	public AbstractBenchmark(String name, Dataset firstInput, Dataset... moreInputs) {
		this.name = name;
		this.inputs = ImmutableList.copyOf(Lists.asList(firstInput, moreInputs));
	}
	public AbstractBenchmark(Dataset firstInput, Dataset... moreInputs) {
		if (!getClass().getSimpleName().isEmpty())
			this.name = getClass().getSimpleName();
		else {
			String binaryName = getClass().getName();
			this.name = binaryName.substring(binaryName.lastIndexOf('.')+1, binaryName.length()-1);
		}
		this.inputs = ImmutableList.copyOf(Lists.asList(firstInput, moreInputs));
	}
	@Override
	public final List<Dataset> inputs() {
		return inputs;
	}
	@Override
	public final String toString() {
		return name;
	}
}
