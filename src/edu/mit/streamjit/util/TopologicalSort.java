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
package edu.mit.streamjit.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 4/30/2013
 */
public final class TopologicalSort {
	private TopologicalSort() {}

	public interface PartialOrder<T> {
		public boolean lessThan(T a, T b);
	}

	public static <T> ImmutableList<T> sort(Iterable<T> data, PartialOrder<? super T> order) {
		return sort(data.iterator(), order);
	}

	public static <T> ImmutableList<T> sort(Iterator<T> iterator, PartialOrder<? super T> order) {
		//A bubble sort.
		List<T> list = Lists.newArrayList(iterator);
		boolean progress;
		do {
			progress = false;
			for (int i = 0; i < list.size(); ++i)
				for (int j = i+1; j < list.size(); ++j)
					if (order.lessThan(list.get(j), list.get(i))) {
						Collections.swap(list, i, j);
						progress = true;
					}
		} while (progress);
		return ImmutableList.copyOf(list);
	}
}
