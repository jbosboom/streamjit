package edu.mit.streamjit.util.json;

import static com.google.common.base.Preconditions.*;
import java.io.StringReader;
import java.util.Map;
import java.util.ServiceLoader;
import javax.json.Json;
import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonStructure;
import javax.json.JsonValue;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/25/2013
 */
public final class Jsonifiers {
	private static final ServiceLoader<JsonifierFactory> FACTORY_LOADER = ServiceLoader.load(JsonifierFactory.class);
	private Jsonifiers() {}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public static JsonValue toJson(Object obj) {
		//This is checked dynamically via obj.getClass().  Not sure what
		//generics I need to make this work.
		Jsonifier jsonifier = findJsonifierByClass(obj.getClass());
		return jsonifier.toJson(obj);
	}

	public static <T> T fromJson(String str, Class<T> klass) {
		checkNotNull(str);
		JsonStructure struct;
		try {
			struct = Json.createReader(new StringReader(str)).read();
		} catch (JsonException ex) {
			throw new JsonSerializationException(ex);
		}
		return fromJson(struct, klass);
	}

	public static <T> T fromJson(JsonValue value, Class<T> klass) {
		Class<?> trueClass = klass;
		if (value instanceof JsonObject)
			trueClass = objectClass((JsonObject)value);

		Jsonifier<?> jsonifier = findJsonifierByClass(trueClass);
		Object obj = jsonifier.fromJson(value);
		return klass.cast(obj);
	}

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

	public static <T> Jsonifier<T> findJsonifierByClass(Class<T> klass) {
		for (JsonifierFactory f : FACTORY_LOADER) {
			Jsonifier<T> jsonifier = f.getJsonifier(klass);
			if (jsonifier != null)
				return jsonifier;
		}
		throw new JsonSerializationException("no Jsonifier for "+klass);
	}

	public static JsonObject checkClassEqual(JsonValue value, Class<?> klass) {
		if (!(value instanceof JsonObject))
			throw new JsonSerializationException("value not an object", value);
		JsonObject obj = (JsonObject)value;
		if (!objectClass(obj).equals(klass))
			throw new JsonSerializationException("class not "+klass.getName(), value);
		return obj;
	}

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
