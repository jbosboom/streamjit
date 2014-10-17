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

import static edu.mit.streamjit.util.bytecode.methodhandles.LookupUtils.findVirtual;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

/**
 * An empty ConcreteStorage that cannot be read or written.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 1/15/2014
 */
public final class EmptyConcreteStorage implements ConcreteStorage {
	private final Class<?> type;
	private final String string;
	public EmptyConcreteStorage(Storage s) {
		this.type = s.type();
		this.string = s.toString();
	}
	@Override
	public Class<?> type() {
		return type;
	}
	@Override
	public Object read(int index) {
		throw new UnsupportedOperationException("reading from empty storage "+string);
	}
	@Override
	public void write(int index, Object data) {
		throw new UnsupportedOperationException("writing to empty storage "+string);
	}
	@Override
	public void adjust() {
	}
	@Override
	public void sync() {
	}
	private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
	private static final MethodHandle READ = findVirtual(LOOKUP, "read");
	private static final MethodHandle WRITE = findVirtual(LOOKUP, "write");
	private static final MethodHandle ADJUST = findVirtual(LOOKUP, "adjust");
	@Override
	public MethodHandle readHandle() {
		return READ.bindTo(this);
	}
	@Override
	public MethodHandle writeHandle() {
		return WRITE.bindTo(this);
	}
	@Override
	public MethodHandle adjustHandle() {
		return ADJUST.bindTo(this);
	}
	@Override
	public String toString() {
		return getClass().getSimpleName()+string;
	}
}
