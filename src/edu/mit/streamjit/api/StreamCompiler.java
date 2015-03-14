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
package edu.mit.streamjit.api;

/**
 * A StreamCompiler compiles a stream graph to a CompiledStream.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 11/20/2012
 */
public interface StreamCompiler {
	/**
	 *
	 * @param <I>
	 * @param <O>
	 * @param stream
	 * @return
	 * @throws IllegalStreamGraphException if the stream graph is illegal or
	 * uses features not supported by this compiler
	 * @throws StreamCompilationFailedException if compilation fails for some
	 * reason other than a stream graph defect
	 */
	public <I, O> CompiledStream compile(OneToOneElement<I, O> stream, Input<I> input, Output<O> output);
}
