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
package edu.mit.streamjit.tuner;

import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmarker;

/**
 * Prints out default configurations given a blob factory class and benchmark.
 * TODO: if the factory needs arguments, pass them in a Configuration object's
 * third parameter?  pass everything in extra data?
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 1/21/2014
 */
public final class ConfigGenerator2 {
	private ConfigGenerator2() {}

	public static void main(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		Class<? extends BlobFactory> factoryClass = Class.forName(args[0]).asSubclass(BlobFactory.class);
		BlobFactory factory = factoryClass.newInstance();
		Benchmark bm = Benchmarker.getBenchmarkByName(args[1]);
		ConnectWorkersVisitor cwv = new ConnectWorkersVisitor();
		bm.instantiate().visit(cwv);
		Configuration config = factory.getDefaultConfiguration(Workers.getAllWorkersInGraph(cwv.getSource()));
		System.out.println(config.toJson());
	}
}
