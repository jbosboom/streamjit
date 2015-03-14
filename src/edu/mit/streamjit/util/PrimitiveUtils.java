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

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.collect.ImmutableMap;

/**
 * Provides utility methods on primitives not provided in wrapper classes, Guava
 * wrapper or primitive utilities, or
 * {@link com.google.common.primitives.Primitives}.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 3/1/2014
 */
public final class PrimitiveUtils {
	private PrimitiveUtils() {}

	private static final ImmutableMap<Class<?>, Integer> SIZEOF_MAP = ImmutableMap.<Class<?>, Integer>builder()
			.put(void.class, 0)
			.put(boolean.class, 1)
			.put(byte.class, 1)
			.put(char.class, 2)
			.put(short.class, 2)
			.put(int.class, 4)
			.put(long.class, 8)
			.put(float.class, 4)
			.put(double.class, 8)
			.build();

	/**
	 * Returns the size of the given primitive type, in bytes.  void is
	 * considered to have size 0.
	 * @param prim a primitive type
	 * @return the size of the given primitive type, in bytes
	 */
	public static int sizeof(Class<?> prim) {
		checkArgument(prim.isPrimitive(), "not a primitive: %s", prim);
		return SIZEOF_MAP.get(prim);
	}
}
