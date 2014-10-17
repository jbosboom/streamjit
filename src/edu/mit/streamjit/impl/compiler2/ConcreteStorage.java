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

import java.lang.invoke.MethodHandle;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An actual buffer or other storage element.  (Contrast with Storage, which is
 * an IR-level object.)
 *
 * This class provides two sets of functions: one that performs an operation
 * directly, and one that provides a MethodHandle that performs the operation
 * when called. Depending on the implementation, one set of operations may be
 * implemented in terms of the other. The direct methods are used primarily
 * during initialization and draining, while the indirect methods provide
 * MethodHandles for generated code. Note that there is no indirect form of
 * {@link #sync()}.
 *
 * Indices used with ConcreteStorage methods have already been translated
 * through index functions ("physical indices"). Readers and writers use the
 * same indices; that is, writers must use offsets to avoid writing at indices
 * that will be read by readers. The {@link #adjust()} method shifts indices
 * towards negative infinity to prepare for the next steady state iteration.
 * (Physical indices may be further adjusted by implementations to account for
 * e.g. circular buffers, but such details are not visible to users of this
 * interface.) Note that indices may be negative.
 *
 * ConcreteStorage implementations used for external Storage must permit
 * multiple concurrent readers and writers (e.g., not throwing
 * {@link ConcurrentModificationException}).  Implementations are guaranteed
 * that indices will be written at most once between calls to adjust() and that
 * indices written will not be read until the next call to sync() or adjust().
 * Further, implementations may assume that calls to sync() and adjust() are
 * synchronized externally with calls to read or write (i.e., there is a
 * global barrier at the end of each steady-state iteration).  Thus
 * implementations may not need any synchronization of their own.  (For example,
 * an implementation using an array will not require synchronization, but a
 * Map<Integer, Object>-based implementation must use {@link ConcurrentHashMap}
 * rather than a plain {@link HashMap} to avoid throwing
 * ConcurrentModificationException.)
 *
 * ConcreteStorage implementations used for internal Storage objects or during
 * the initialization schedule are used only within a single thread, so they
 * need not worry about synchronization; normal happens-before ordering in a
 * single thread is enough to ensure readers see preceding writes.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 10/10/2013
 */
public interface ConcreteStorage {
	/**
	 * Returns the type of elements stored in this ConcreteStorage.
	 * @return the type of elements stored in this ConcreteStorage
	 */
	public Class<?> type();

	/**
	 * Returns the element at the given index, boxed if necessary.
	 * @param index the index to read
	 * @return the element at the given index
	 */
	public default Object read(int index) {
		try {
			return readHandle().invoke(index);
		} catch (Throwable ex) {
			throw new AssertionError(String.format("%s.read(%d)", this, index), ex);
		}
	}
	/**
	 * Writes the given element at the given index, unboxing if necessary.
	 * @param index the index to write
	 * @param data the element to write
	 */
	public default void write(int index, Object data) {
		try {
			writeHandle().invoke(index, data);
		} catch (Throwable ex) {
			throw new AssertionError(String.format("%s.write(%d, %s)", this, index, data), ex);
		}
	}

	/**
	 * Shifts indices toward negative infinity and ensures that subsequent calls
	 * to read will see items written by previous calls to write.  (These are
	 * the end-of-steady-state adjustments.)
	 */
	public void adjust();
	/**
	 * Ensures that subsequent calls to read will see items written by previous
	 * calls to write, but does not adjust indices.  Despite the name, this
	 * method need not perform any synchronization actions unless required by
	 * the implementation.
	 */
	public void sync();

	/**
	 * Returns a MethodHandle of int -> T type, where T is the type of elements
	 * stored in this storage, that maps a physical index to the element stored
	 * at that index.
	 * @return a handle that reads from this storage
	 */
	public MethodHandle readHandle();
	/**
	 * Returns a MethodHandle of int, T -> void type, where T is the type of
	 * elements stored in this storage, that stores an element at the given
	 * physical index.
	 * @return a handle that writes to this storage
	 */
	public MethodHandle writeHandle();
	/**
	 * Returns a MethodHandle of void -> void type that shifts indices toward
	 * negative infinity and ensures that subsequent calls to read will see
	 * items written by previous calls to write.
	 * @return a handle that adjusts this storage
	 */
	public MethodHandle adjustHandle();
}
