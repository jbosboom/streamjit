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
package edu.mit.streamjit.impl.interp;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An EmptyChannel is a capacity-bounded channel with a capacity of 0; that is,
 * it is always empty.
 * @param <E> the type of elements (not) in this channel
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 1/8/2013
 */
public final class EmptyChannel<E> implements Channel<E> {
	@Override
	public void push(E element) {
		throw new IllegalStateException();
	}

	@Override
	public E peek(int index) {
		throw new IndexOutOfBoundsException();
	}

	@Override
	public E pop() {
		throw new NoSuchElementException();
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public boolean isEmpty() {
		return true;
	}

	@Override
	public Iterator<E> iterator() {
		return Collections.emptyIterator();
	}
}
