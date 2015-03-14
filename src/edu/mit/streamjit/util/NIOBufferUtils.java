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
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;

/**
 * Utility methods for working with NIO Buffers.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 2/28/2014
 */
public final class NIOBufferUtils {
	private NIOBufferUtils() {}


	private static final ImmutableBiMap<Class<?>, Class<? extends Buffer>> PRIM_TO_BUF_MAP = ImmutableBiMap.<Class<?>, Class<? extends Buffer>>builder()
//				.put(boolean.class, ByteBuffer.class)
				.put(byte.class, ByteBuffer.class)
				.put(char.class, CharBuffer.class)
				.put(short.class, ShortBuffer.class)
				.put(int.class, IntBuffer.class)
				.put(long.class, LongBuffer.class)
				.put(float.class, FloatBuffer.class)
				.put(double.class, DoubleBuffer.class)
				.build();
	private static final ImmutableBiMap<Class<? extends Buffer>, Class<?>> BUF_TO_PRIM_MAP = PRIM_TO_BUF_MAP.inverse();

	/**
	 * Allocates a direct buffer of the given type large enough to hold the
	 * given number of elements.
	 * @param <T> the buffer type (e.g. FloatBuffer)
	 * @param bufferType the buffer type token (e.g. FloatBuffer.class)
	 * @param elements the requested buffer size (in elements, not bytes)
	 * @return a direct buffer with the given size
	 * @see #bufferForPrimitive(java.lang.Class)
	 */
	public static <T extends Buffer> T allocateDirect(Class<T> bufferType, int elements) {
		checkArgument(!bufferType.equals(Buffer.class), "must specify specific buffer type");
		checkArgument(elements >= 0, "%s", elements);
		ByteBuffer buffer = ByteBuffer.allocateDirect(
				elements * PrimitiveUtils.sizeof(BUF_TO_PRIM_MAP.get(bufferType)))
				.order(ByteOrder.nativeOrder());
		Buffer ret;
		if (bufferType == CharBuffer.class) ret = buffer.asCharBuffer();
		else if (bufferType == ShortBuffer.class) ret = buffer.asShortBuffer();
		else if (bufferType == IntBuffer.class) ret = buffer.asIntBuffer();
		else if (bufferType == LongBuffer.class) ret = buffer.asLongBuffer();
		else if (bufferType == FloatBuffer.class) ret = buffer.asFloatBuffer();
		else if (bufferType == DoubleBuffer.class) ret = buffer.asDoubleBuffer();
		else ret = buffer;
		return bufferType.cast(ret);
	}

	/**
	 * Returns the Buffer subclass corresponding to the given primitive type.
	 * Note there is no Buffer subclass for boolean or void.
	 * @param prim a primitive type that isn't boolean or void
	 * @return the given type's Buffer subclass
	 */
	public static Class<? extends Buffer> bufferForPrimitive(Class<?> prim) {
		checkArgument(prim.isPrimitive(), "not a primitive: %s", prim);
		checkArgument(!prim.equals(void.class) && !prim.equals(boolean.class), "no buffer for %s", prim);
		return PRIM_TO_BUF_MAP.get(prim);
	}

	/**
	 * Returns the primitive type contained in the given buffer subclass.
	 * @param bufferType a Buffer subclass
	 * @return the primitive type stored in the given Buffer subclass
	 */
	public static Class<?> primitiveInBuffer(Class<? extends Buffer> bufferType) {
		checkArgument(!bufferType.equals(Buffer.class), "must specify specific buffer type");
		return BUF_TO_PRIM_MAP.get(bufferType);
	}
}
