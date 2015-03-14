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
package edu.mit.streamjit.util.json;

import com.google.common.collect.ImmutableMap;
import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import javax.json.Json;
import javax.json.JsonNumber;
import javax.json.JsonString;
import javax.json.JsonValue;

/**
 * The factory for primitive Jsonifiers.  Also contains those Jsonifiers as
 * nested classes.
 *
 * This class is public only so that ServiceLoader can instantiate it.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 3/25/2013
 */
@ServiceProvider(value = JsonifierFactory.class, priority = Integer.MIN_VALUE)
public final class PrimitiveJsonifierFactory implements JsonifierFactory {
	/**
	 * Public constructor provided for ServiceLoader.  Don't instantiate this
	 * class directly; use the methods in Jsonifiers instead.
	 */
	public PrimitiveJsonifierFactory() {}

	private static final ImmutableMap<Class<?>, Jsonifier<?>> MAP = ImmutableMap.<Class<?>, Jsonifier<?>>builder()
			.put(Boolean.class, new BooleanJsonifier())
			.put(Integer.class, new IntegerJsonifier())
			.put(Long.class, new LongJsonifier())
			.put(BigInteger.class, new BigIntegerJsonifier())
			.put(BigDecimal.class, new BigDecimalJsonifier())
			.put(String.class, new StringJsonifier())
			.put(Class.class, new ClassJsonifier())
			.build();

	@Override
	@SuppressWarnings("unchecked")
	public <T> Jsonifier<T> getJsonifier(Class<T> klass) {
		return (Jsonifier<T>)MAP.get(klass);
	}

	private static class BooleanJsonifier implements Jsonifier<Boolean> {
		@Override
		public Boolean fromJson(JsonValue value) {
			switch (value.getValueType()) {
				case TRUE:
					return true;
				case FALSE:
					return false;
				default:
					throw new JsonSerializationException("Not a Boolean value", value);
			}
		}
		@Override
		public JsonValue toJson(Boolean t) {
			return t ? JsonValue.TRUE : JsonValue.FALSE;
		}
	}

	private static abstract class NumberJsonifier<T extends Number> implements Jsonifier<T> {
		@Override
		public T fromJson(JsonValue value) {
			try {
				return get(value);
			} catch (ClassCastException | ArithmeticException ex) {
				throw new JsonSerializationException(null, ex, value);
			}
		}
		@Override
		public JsonValue toJson(T t) {
			return Json.createReader(new StringReader("["+t+"]")).readArray().getJsonNumber(0);
		}
		protected abstract T get(JsonValue value);
	}

	private static class IntegerJsonifier extends NumberJsonifier<Integer> {
		@Override
		protected Integer get(JsonValue value) {
			return ((JsonNumber)value).intValueExact();
		}
	}

	private static class LongJsonifier extends NumberJsonifier<Long> {
		@Override
		protected Long get(JsonValue value) {
			return ((JsonNumber)value).longValueExact();
		}
	}

	private static class BigIntegerJsonifier extends NumberJsonifier<BigInteger> {
		@Override
		protected BigInteger get(JsonValue value) {
			return ((JsonNumber)value).bigIntegerValueExact();
		}
	}

	private static class BigDecimalJsonifier extends NumberJsonifier<BigDecimal> {
		@Override
		protected BigDecimal get(JsonValue value) {
			return ((JsonNumber)value).bigDecimalValue();
		}
	}

	private static class StringJsonifier implements Jsonifier<String> {
		@Override
		public String fromJson(JsonValue value) {
			try {
				return ((JsonString)value).getString();
			} catch (ClassCastException ex) {
				throw new JsonSerializationException(null, ex, value);
			}
		}
		@Override
		public JsonValue toJson(String t) {
			return Json.createArrayBuilder().add(t).build().get(0);
		}
	}

	private static class ClassJsonifier implements Jsonifier<Class<?>> {
		@Override
		public Class<?> fromJson(JsonValue value) {
			try {
				return Class.forName(((JsonString)value).getString());
			} catch (ClassCastException | ClassNotFoundException ex) {
				throw new JsonSerializationException(null, ex, value);
			}
		}
		@Override
		public JsonValue toJson(Class<?> t) {
			return Jsonifiers.toJson(t.getName());
		}
	}

	public static void main(String[] args) {
		System.out.println(Jsonifiers.toJson(true));
		System.out.println(Jsonifiers.toJson(false));
		System.out.println(Jsonifiers.toJson(35));
	}
}
