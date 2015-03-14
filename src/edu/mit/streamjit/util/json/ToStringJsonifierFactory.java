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
import com.google.common.reflect.Invokable;
import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonValue;

/**
 * A JsonifierFactory that serializes objects with a toString() method and
 * factory method or constructor taking a String.
 * <p/>
 * Specifically, the class must:
 * <ul>
 * <li>Override toString().</li>
 * <li>Have a static method returning an instance of the class and compatible
 * with a single String argument, or have a constructor compatible with a single
 * String argument. Private methods or constructors will be selected if
 * {@link java.lang.reflect.AccessibleObject#setAccessible(boolean)} succeeds;
 * if it does not, that method or constructor will be skipped and the search
 * will continue. If a class has multiple such methods or constructors, the one
 * chosen is unspecified and may change from implementation to implementation or
 * run to run of the same implementation.</li>
 * </ul>
 * <p/>
 * Note that the above rules cannot guarantee that all of the object's state is
 * saved, as the object's toString or factory method/constructor may lose
 * information.
 * <p/>
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 3/29/2013
 */
@ServiceProvider(value = JsonifierFactory.class, priority = Integer.MAX_VALUE-1)
public final class ToStringJsonifierFactory implements JsonifierFactory {
	/**
	 * Sentinel to indicate that a class is not supported by this factory, so we
	 * can distinguish it from null when calling cache.get().
	 */
	private static final Jsonifier<?> SENTINEL = new Sentinel();
	/**
	 * Maps Class<T> to Jsonifier<T>, which may be a Sentinel.
	 */
	private final ConcurrentMap<Class<?>, Jsonifier<?>> cache = new ConcurrentHashMap<>();

	@Override
	@SuppressWarnings("unchecked")
	public <T> Jsonifier<T> getJsonifier(Class<T> klass) {
		Jsonifier<?> jsonifier = cache.get(klass);
		if (jsonifier == null) {
			Invokable<?, T> factory = findFactory(klass);
			jsonifier = factory != null ? new ToStringJsonifier<>(klass, factory) : SENTINEL;
			cache.put(klass, jsonifier);
		}
		return (Jsonifier<T>)(jsonifier != SENTINEL ? jsonifier : null);
	}

	/**
	 * Finds the factory method or constructor to be used to construct Ts from
	 * Strings.
	 * @param <T> the type of the class
	 * @param klass the class
	 * @return a factory method or constructor, or null
	 */
	private static <T> Invokable<?, T> findFactory(Class<T> klass) {
		//The class must override toString().
		Method toString;
		try {
			toString = klass.getMethod("toString");
		} catch (NoSuchMethodException ex) {
			//Interfaces, maybe primitives, etc.
			return null;
		}
		if (!toString.getDeclaringClass().equals(klass))
			return null;

		//I'd like to condense these two loops into one loop over Invokeables,
		//but: 1) I want only static methods, but constructors are never
		//marked static; 2) Invokable hides the this argument to inner class
		//constructors, causing possible misdetection.
		for (Method method : klass.getDeclaredMethods()) {
			final int modifiers = method.getModifiers();
			if (!Modifier.isStatic(modifiers))
				continue;
			if (Modifier.isAbstract(modifiers))
				continue;
			if (!method.getReturnType().equals(klass))
				continue;
			Class<?>[] parameters = method.getParameterTypes();
			if (parameters.length != 1)
				continue;
			if (!parameters[0].isAssignableFrom(String.class))
				continue;
			if (!Modifier.isPublic(modifiers))
				try {
					method.setAccessible(true);
				} catch (SecurityException ex) {
					continue;
				}
			return Invokable.from(method).returning(klass);
		}
		for (Constructor<?> ctor : klass.getDeclaredConstructors()) {
			Class<?>[] parameters = ctor.getParameterTypes();
			if (parameters.length != 1)
				continue;
			if (!parameters[0].isAssignableFrom(String.class))
				continue;
			if (!Modifier.isPublic(ctor.getModifiers()))
				try {
					ctor.setAccessible(true);
				} catch (SecurityException ex) {
					continue;
				}
			//See the comment on Class.getConstructors(), which also applies to
			//Class.getDeclaredConstructors.
			@SuppressWarnings("unchecked")
			Constructor<T> ctorT = (Constructor<T>)ctor;
			return Invokable.from(ctorT);
		}
		return null;
	}

	private static final class ToStringJsonifier<T> implements Jsonifier<T> {
		private final Class<T> klass;
		private final Invokable<?, T> factory;
		private ToStringJsonifier(Class<T> klass, Invokable<?, T> factory) {
			this.klass = klass;
			this.factory = factory;
		}
		@Override
		public T fromJson(JsonValue value) {
			JsonObject obj = Jsonifiers.checkClassEqual(value, klass);
			try {
				return factory.invoke(null, obj.getString("data"));
			} catch (InvocationTargetException | IllegalAccessException ex) {
				throw new JsonSerializationException(null, ex, value);
			}
		}
		@Override
		public JsonValue toJson(T t) {
			checkArgument(t.getClass() == klass, "%s is not a %s", t.getClass().getName(), klass.getName());
			return Json.createObjectBuilder()
				.add("class", Jsonifiers.toJson(t.getClass()))
				.add("data", t.toString())
				.build();
		}
	}

	private static final class Sentinel implements Jsonifier<Object> {
		@Override
		public Object fromJson(JsonValue value) {
			throw new AssertionError();
		}
		@Override
		public JsonValue toJson(Object t) {
			throw new AssertionError();
		}
	}

	public static void main(String[] args) throws Throwable {
		Jsonifier<String> j = new ToStringJsonifierFactory().getJsonifier(String.class);
		System.out.println(j.toJson("jit in your jit"));
		System.out.println(j.fromJson(j.toJson("jit in your jit")));
	}
}
