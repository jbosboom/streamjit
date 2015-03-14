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

import static com.google.common.base.Preconditions.*;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Map;
import java.util.ServiceLoader;
import javax.json.Json;
import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonStructure;
import javax.json.JsonValue;
import javax.json.JsonWriter;
import javax.json.JsonWriterFactory;
import javax.json.stream.JsonGenerator;

/**
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 3/25/2013
 */
public final class Jsonifiers {
	private static final ServiceLoader<JsonifierFactory> FACTORY_LOADER = ServiceLoader.load(JsonifierFactory.class);
	private Jsonifiers() {}

	/**
	 * Serializes the given Object to JSON.
	 *
	 * This method returns JsonValue; to get a JSON string, call the returned
	 * value's toString() method.
	 * @param obj an object
	 * @return a JsonValue representing the serialized form of the object
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public static JsonValue toJson(Object obj) {
		if (obj == null)
			return JsonValue.NULL;
		//This is checked dynamically via obj.getClass().  Not sure what
		//generics I need to make this work.
		Jsonifier jsonifier = findJsonifierByClass(obj.getClass());
		return jsonifier.toJson(obj);
	}

	/**
	 * Deserializes the given well-formed JSON string into an object of the given class.  If
	 * the object is of a subtype of the given class, an instance of that
	 * subtype will be created, rather than an instance of the given class.
	 *
	 * Note that well-formed JSON strings are always arrays or objects at their
	 * top level; this method will not deserialize the number 3 to an Integer,
	 * for example.
	 * @param <T> the type of the given class
	 * @param str a well-formed JSON string
	 * @param klass the class of the value being deserialized (must not be null)
	 * @return a deserialized Java object
	 * @throws NullPointerException if either str or klass is null
	 * @throws JsonSerializationException if a deserialization error occurs
	 */
	public static <T> T fromJson(String str, Class<T> klass) {
		checkNotNull(str);
		checkNotNull(klass);
		JsonStructure struct;
		try {
			struct = Json.createReader(new StringReader(str)).read();
		} catch (JsonException ex) {
			throw new JsonSerializationException(ex);
		}
		return fromJson(struct, klass);
	}

	/**
	 * Deserializes the given JSON value into an object of the given class.  If
	 * the object is of a subtype of the given class, an instance of that
	 * subtype will be created, rather than an instance of the given class.
	 * @param <T> the type of the given class
	 * @param value the JSON value to deserialized (must not be null, but may
	 * be a JSON null (JsonValue.NULL) object)
	 * @param klass the class of the value being deserialized (must not be null)
	 * @return a deserialized Java object
	 * @throws NullPointerException if either value or klass is null
	 * @throws JsonSerializationException if a deserialization error occurs
	 */
	public static <T> T fromJson(JsonValue value, Class<T> klass) {
		checkNotNull(value); //checks for Java null, not JSON null
		checkNotNull(klass);

		if (value.getValueType() == JsonValue.ValueType.NULL)
			//Surprisingly, int.class.cast(null) does not throw, instead
			//returning null, which is probably not what the caller expects
			//(if the caller tries to unbox immediately, it'll receive a
			//NullPointerException for the implicit call to intValue().)  So we
			//check explicitly.
			if (klass.isPrimitive())
				throw new JsonSerializationException("deserializing null as "+klass.getSimpleName());
			else
				return klass.cast(null);

		Class<?> trueClass = klass;
		if (value instanceof JsonObject)
			trueClass = objectClass((JsonObject)value);

		Jsonifier<?> jsonifier = findJsonifierByClass(trueClass);
		Object obj = jsonifier.fromJson(value);
		return klass.cast(obj);
	}

	/**
	 * Parses the given JSON string, then prettyprints it.  This is a purely
	 * syntactic transformation; no serialization or deserialization is done.
	 * @param json a well-formed JSON string
	 * @return a pretty-printed JSON string
	 */
	public static String prettyprint(String json) {
		return prettyprint(Json.createReader(new StringReader(json)).read());
	}

	/**
	 * Prettyprints the given JSON structure.  (All well-formed JSON strings
	 * have an array or object at their top level.)
	 * @param value a JSON structure
	 * @return a pretty-printed JSON string
	 */
	public static String prettyprint(JsonStructure value) {
		StringWriter string = new StringWriter();
		JsonWriterFactory writerFactory = Json.createWriterFactory(Collections.singletonMap(JsonGenerator.PRETTY_PRINTING, null));
		try (JsonWriter writer = writerFactory.createWriter(string)) {
			writer.write(value);
		}
		return string.toString();
	}

	/**
	 * Extracts the class of a JSON object (the value of the "class" key), or
	 * throws JsonSerializationException if unsuccessful.  Used during
	 * deserialization.
	 * @param obj a JSON object
	 * @return the class of the JSON object
	 * @throws JsonSerializationException if the "class" key is not present, or
	 * if the value is not a string or does not name a Java class
	 */
	private static Class<?> objectClass(JsonObject obj) {
		if (!(obj.get("class") instanceof JsonString))
			throw new JsonSerializationException("class not present or not a string", obj);
		String className = obj.getString("class");
		Class<?> klass;
		try {
			klass = Class.forName(className);
		} catch (ClassNotFoundException ex) {
			throw new JsonSerializationException("bad class: "+className, ex, obj);
		}
		return klass;
	}

	private static <T> Jsonifier<T> findJsonifierByClass(Class<T> klass) {
		for (JsonifierFactory f : FACTORY_LOADER) {
			Jsonifier<T> jsonifier = f.getJsonifier(klass);
			if (jsonifier != null)
				return jsonifier;
		}
		throw new JsonSerializationException("no Jsonifier for "+klass);
	}

	/**
	 * Checks if the given value is a JSON object representing a Java object of
	 * the given class.  If it is, the JSON object is returned; if not, a
	 * JsonSerializationException is thrown.
	 *
	 * This method is provided for the benefit of Jsonifier implementations;
	 * users of the JSON library have no need to call this method.
	 *
	 * Jsonifiers can call this method to test if the object they are
	 * deserializing is an instance of the exact class they expect.
	 * @param value a JSON value
	 * @param klass a class
	 * @return the JSON object if it represents a Java object of the given class
	 * @throws JsonSerializationException if the JSON value is not a JSON object
	 * or represents a Java object of a different class
	 * @see #checkClassAssignable(javax.json.JsonValue, java.lang.Class)
	 */
	public static JsonObject checkClassEqual(JsonValue value, Class<?> klass) {
		if (!(value instanceof JsonObject))
			throw new JsonSerializationException("value not an object", value);
		JsonObject obj = (JsonObject)value;
		if (!objectClass(obj).equals(klass))
			throw new JsonSerializationException("class not "+klass.getName(), value);
		return obj;
	}

	/**
	 * Checks if the given value is a JSON object representing a Java object assignable to
	 * the given class.  If it is, the JSON object is returned; if not, a
	 * JsonSerializationException is thrown.
	 *
	 * This method is provided for the benefit of Jsonifier implementations;
	 * users of the JSON library have no need to call this method.
	 *
	 * Jsonifiers can call this method to test if the object they are
	 * deserializing is an instance or subtype of the class they expect.
	 * @param value a JSON value
	 * @param klass a class
	 * @return the JSON object if it represents a Java object assignable to the given class
	 * @throws JsonSerializationException if the JSON value is not a JSON object
	 * or represents a Java object of a different class
	 * @see #checkClassEqual(javax.json.JsonValue, java.lang.Class)
	 */
	public static JsonObject checkClassAssignable(JsonValue value, Class<?> klass) {
		if (!(value instanceof JsonObject))
			throw new JsonSerializationException("value not an object", value);
		JsonObject obj = (JsonObject)value;
		if (!klass.isAssignableFrom(objectClass(obj)))
			throw new JsonSerializationException("class not "+klass.getName(), value);
		return obj;
	}

	public static boolean notHeapPolluted(Iterable<?> iterable, Class<?> klass) {
		for (Object o : iterable)
			if (!klass.isAssignableFrom(o.getClass()))
				return false; //heap-polluted
		return true;
	}

	public static boolean notHeapPolluted(Map<?, ?> map, Class<?> keyClass, Class<?> valueClass) {
		return notHeapPolluted(map.keySet(), keyClass) && notHeapPolluted(map.values(), valueClass);
	}
}
