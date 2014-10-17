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

import com.google.common.base.Supplier;
import java.lang.invoke.MethodHandle;
import java.util.NavigableSet;

/**
 * Transforms index functions into equivalent (but possibly more performant)
 * handles.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 12/8/2013
 */
public interface IndexFunctionTransformer {
	/**
	 * Returns a handle equivalent to the given handle when evaluated over the
	 * given domain.
	 * @param fxn the function to transform
	 * @param domain the domain the function will be evaluated over
	 * @return an equivalent handle
	 */
	public MethodHandle transform(MethodHandle fxn, Supplier<? extends NavigableSet<Integer>> domain);
}
