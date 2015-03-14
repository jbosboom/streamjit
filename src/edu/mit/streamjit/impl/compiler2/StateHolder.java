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

import com.google.common.collect.ImmutableMap;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.compiler2.Compiler2BlobHost.DrainInstruction;
import edu.mit.streamjit.util.ReflectionUtils;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;

/**
 * Subclasses of this class hold worker state.
 * TODO: base iteration count as pseudo-state
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 12/11/2013
 */
public abstract class StateHolder implements DrainInstruction {
	private final Worker<?, ?> worker;
	protected StateHolder(Worker<?, ?> worker) {
		this.worker = worker;
	}

	/**
	 * Rather than produce data, "drain" by moving any non-final fields back
	 * into the worker.  (They'll then be used by the interpreter during
	 * draining and eventually collected into a DrainData.)
	 * @return an empty immutable map
	 */
	@Override
	public Map<Blob.Token, Object[]> call() {
		for (Field hf : getClass().getDeclaredFields()) {
			Field wf = ReflectionUtils.getFieldByName(worker, hf.getName());
			if (!Modifier.isFinal(wf.getModifiers()))
				try {
					wf.setAccessible(true);
					wf.set(worker, hf.get(this));
				} catch (IllegalAccessException ex) {
					throw new AssertionError(ex);
				}
		}
		return ImmutableMap.of();
	}
}
