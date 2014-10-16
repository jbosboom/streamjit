package edu.mit.streamjit.util.json;

import javax.json.JsonValue;

/**
 * A Jsonifier converts objects of a type T into JsonValues and vice versa.
 *
 * Instances of this class should be thread-safe and reentrant; that is, methods
 * on this class may be called simultaneously by any number of threads,
 * including multiple times from a single thread (recursively).
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 3/25/2013
 */
public interface Jsonifier<T> {
	public T fromJson(JsonValue value);
	public JsonValue toJson(T t);
}
