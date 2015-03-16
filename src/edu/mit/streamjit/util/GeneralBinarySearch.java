/*
 * Copyright (c) 2015 Massachusetts Institute of Technology
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

import java.util.function.IntPredicate;

/**
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 3/16/2015
 */
public final class GeneralBinarySearch {
	private GeneralBinarySearch() {}

	public static int binarySearch(IntPredicate predicate, int lowerInclusive, int upperExclusive) {
		if (lowerInclusive == upperExclusive) {
			assert !predicate.test(lowerInclusive);
			return lowerInclusive;
		}
		int mid = (lowerInclusive + upperExclusive)/2;
		if (predicate.test(mid)) {
			return binarySearch(predicate, mid+1, upperExclusive);
		} else {
			return binarySearch(predicate, lowerInclusive, mid);
		}
	}

	public static int binarySearch(IntPredicate predicate, int lowerInclusive) {
		//multiplying negative numbers by 2 won't work here.
		if (lowerInclusive < 0) throw new UnsupportedOperationException();
		int upperExclusive = lowerInclusive+1;
		while (predicate.test(upperExclusive)) {
			lowerInclusive = Math.incrementExact(upperExclusive);
			upperExclusive = Math.multiplyExact(upperExclusive, 2);
		}
		return binarySearch(predicate, lowerInclusive, upperExclusive);
	}

	public static void main(String[] args) {
		System.out.println(binarySearch(x -> x < 5, 0, 100));
		System.out.println(binarySearch(x -> x < 5, 5, 6));
		System.out.println(binarySearch(x -> x < 5, 6, 6));
		System.out.println(binarySearch(x -> x < 5, 6, 7));
		System.out.println(binarySearch(x -> x <= 1000000, 0));
	}
}
