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

import com.google.common.primitives.Primitives;
import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import java.lang.reflect.Array;
import java.util.Arrays;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonValue;

/**
 * ArrayJsonifierFactory converts (possibly nested) arrays to and from JSON.
 * Primitive arrays are supported.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 3/27/2013
 */
@ServiceProvider(value = JsonifierFactory.class, priority = Integer.MIN_VALUE+1)
public final class ArrayJsonifierFactory implements JsonifierFactory {
	@Override
	@SuppressWarnings({"unchecked","rawtypes"})
	public <T> Jsonifier<T> getJsonifier(Class<T> klass) {
		if (klass.isArray())
			return new ArrayJsonifier(klass.getComponentType());
		return null;
	}

	/**
	 * In order to work with arrays of objects and primitives at the same time,
	 * we do everything in terms of Object and use java.lang.reflect.Array to
	 * get, store, and create arrays for us.  This might be a bit slow, but it's
	 * much more maintainable than having a separate Jsonifier for each
	 * primitive type.
	 */
	@SuppressWarnings("rawtypes")
	private static final class ArrayJsonifier implements Jsonifier {
		private final Class<?> componentType;
		/**
		 * It isn't possible to write a Jsonifier for primitive types, so we
		 * have to use the wrapper class when deserializing.  Array then takes
		 * care of unwrapping when storing in the primitive array.
		 */
		private final Class<?> jsonifierType;
		private ArrayJsonifier(Class<?> componentType) {
			assert componentType != null;
			this.componentType = componentType;
			if (componentType.isPrimitive())
				this.jsonifierType = Primitives.wrap(componentType);
			else
				this.jsonifierType = componentType;
		}
		@Override
		public Object fromJson(JsonValue value) {
			JsonArray jsonArray = (JsonArray)value;
			Object array = Array.newInstance(componentType, jsonArray.size());
			for (int i = 0; i < jsonArray.size(); ++i)
				Array.set(array, i, Jsonifiers.fromJson(jsonArray.get(i), jsonifierType));
			return array;
		}
		@Override
		public JsonValue toJson(Object t) {
			JsonArrayBuilder builder = Json.createArrayBuilder();
			int length = Array.getLength(t);
			for (int i = 0; i < length; ++i)
				builder.add(Jsonifiers.toJson(Array.get(t, i)));
			return builder.build();
		}
	}

	public static void main(String[] args) {
		Integer[] boxed = new Integer[]{1, 2};
		String boxedJson = Jsonifiers.toJson(boxed).toString();
		System.out.println(boxedJson);
		Integer[] boxedDeserialized = Jsonifiers.fromJson(boxedJson, Integer[].class);
		System.out.println(Arrays.toString(boxedDeserialized));

		int[] unboxed = new int[]{1, 2};
		String unboxedJson = Jsonifiers.toJson(unboxed).toString();
		System.out.println(unboxedJson);
		int[] unboxedDeserialized = Jsonifiers.fromJson(unboxedJson, int[].class);
		System.out.println(Arrays.toString(unboxedDeserialized));

		int[][] nested = {{1, 2}, {3, 4}};
		String nestedJson = Jsonifiers.toJson(nested).toString();
		System.out.println(nestedJson);
		int[][] nestedDeserialized = Jsonifiers.fromJson(nestedJson, int[][].class);
		System.out.println(Arrays.deepToString(nestedDeserialized));
	}
}
