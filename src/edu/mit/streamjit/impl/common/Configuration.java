/*
 * Copyright (c) 2013-2015 Massachusetts Institute of Technology
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
package edu.mit.streamjit.impl.common;

import static com.google.common.base.Preconditions.*;
import com.google.common.base.Strings;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.math.DoubleMath;
import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.Identity;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.common.Configuration.PartitionParameter.BlobSpecifier;
import edu.mit.streamjit.impl.interp.Interpreter;
import edu.mit.streamjit.util.ReflectionUtils;
import edu.mit.streamjit.util.json.Jsonifier;
import edu.mit.streamjit.util.json.JsonifierFactory;
import edu.mit.streamjit.util.json.Jsonifiers;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;

/**
 * A Configuration contains parameters that can be manipulated by the autotuner
 * (or other things).
 *
 * Instances of this class are immutable.  This class uses the builder pattern;
 * to create a Configuration, get a builder by calling Configuration.builder(),
 * add parameters or subconfigurations to it, then call the builder's build()
 * method to build the configuration.
 *
 * Unless otherwise specified, passing null or an empty string to this class'
 * or any parameter class' methods will result in a NullPointerException being
 * thrown.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 3/23/2013
 */
public final class Configuration {
	private final ImmutableMap<String, Parameter> parameters;
	private final ImmutableMap<String, Configuration> subconfigurations;
	private final ImmutableMap<String, Object> extraData;
	private Configuration(ImmutableMap<String, Parameter> parameters, ImmutableMap<String, Configuration> subconfigurations, ImmutableMap<String, Object> extraData) {
		//We're only called by the builder, so assert, not throw IAE.
		assert parameters != null;
		assert subconfigurations != null;
		this.parameters = parameters;
		this.subconfigurations = subconfigurations;
		this.extraData = extraData;
	}

	/**
	 * Builds Configuration instances.  Parameters and subconfigurations can be
	 * added or removed from this builder; calling build() creates a
	 * Configuration from the current builder state.  Note that build() may be
	 * called more than once; combined with clone(), this allows creating
	 * "prototype" builders that can be cloned, customized, and built.
	 */
	public static final class Builder implements Cloneable {
		private final Map<String, Parameter> parameters;
		private final Map<String, Configuration> subconfigurations;
		private final Map<String, Object> extraData;
		/**
		 * Constructs a new Builder.  Called only by Configuration.build().
		 */
		private Builder() {
			//Type inference fail.
			//These maps have their contents copied in the other constructor, so
			//just use these singleton empty maps.
			this(ImmutableMap.<String, Parameter>of(), ImmutableMap.<String, Configuration>of(), ImmutableMap.<String, Object>of());
		}

		/**
		 * Constructs a new Builder with the given parameters and
		 * subconfigurations.  Called only by Builder.clone() and
		 * Configuration.builder(Configuration).
		 * @param parameters the parameters
		 * @param subconfigurations the subconfigurations
		 */
		private Builder(Map<String, Parameter> parameters, Map<String, Configuration> subconfigurations, Map<String, Object> extraData) {
			//Only called by our own code, so assert.
			assert parameters != null;
			assert subconfigurations != null;
			this.parameters = new HashMap<>(parameters);
			this.subconfigurations = new HashMap<>(subconfigurations);
			this.extraData = new HashMap<>(extraData);
		}

		/**
		 * Adds a parameter to this builder.
		 * @param parameter the parameter to add (cannot be null)
		 * @return this
		 * @throws NullPointerException if parameter is null
		 * @throws IllegalArgumentException if this builder already contains a
		 * parameter with the given name
		 */
		public Builder addParameter(Parameter parameter) {
			checkNotNull(parameter);
			//The parameter constructor should enforce this, so assert.
			assert !Strings.isNullOrEmpty(parameter.getName()) : parameter;
			checkArgument(!parameters.containsKey(parameter.getName()), "conflicting names %s %s", parameters.get(parameter.getName()), parameters);
			parameters.put(parameter.getName(), parameter);
			return this;
		}

		/**
		 * Removes and returns the parameter with the given name from this
		 * builder, or returns null if this builder doesn't contain a parameter
		 * with that name.
		 * @param name the name of the parameter to remove
		 * @return the removed parameter, or null
		 */
		public Parameter removeParameter(String name) {
			return parameters.remove(checkNotNull(Strings.emptyToNull(name)));
		}

		/**
		 * Adds a subconfiguration to this builder.
		 * @param name the name of the subconfiguration to add (cannot be null)
		 * @param subconfiguration the subconfiguration to add (cannot be null)
		 * @return this
		 * @throws NullPointerException if name is null or the empty string, or
		 * if subconfiguration is null
		 * @throws IllegalArgumentException if this builder already contains a
		 * subconfiguration with the given name
		 */
		public Builder addSubconfiguration(String name, Configuration subconfiguration) {
			checkNotNull(Strings.emptyToNull(name));
			checkNotNull(subconfiguration);
			checkArgument(!subconfigurations.containsKey(name), "name %s already in use", name);
			subconfigurations.put(name, subconfiguration);
			return this;
		}

		/**
		 * Removes and returns the subconfiguration with the given name from
		 * this builder, or returns null if this builder doesn't contain a
		 * subconfiguration with that name.
		 * @param name the name of the subconfiguration to remove
		 * @return the removed subconfiguration, or null
		 */
		public Configuration removeSubconfiguration(String name) {
			return subconfigurations.remove(checkNotNull(Strings.emptyToNull(name)));
		}

		/**
		 * Adds extra data to this builder, replacing any data with the same
		 * name.
		 * @param name the name of the extra data to put (cannot be null)
		 * @param data the extra data (cannot be null)
		 * @return this
		 * @throws NullPointerException if name is null or the empty string, or
		 * if data is null
		 */
		public Builder putExtraData(String name, Object data) {
			checkNotNull(Strings.emptyToNull(name));
			checkNotNull(data);
			extraData.put(name, data);
			return this;
		}

		/**
		 * Builds a new Configuration from the parameters and subconfigurations
		 * added to this builder.  This builder is still valid and may be used
		 * to build more configurations (perhaps after adding or removing
		 * elements), but the returned configurations remain immutable.
		 * @return a new Configuration containing the parameters and
		 * subconfigurations added to this builder
		 */
		public Configuration build() {
			return new Configuration(ImmutableMap.copyOf(parameters), ImmutableMap.copyOf(subconfigurations), ImmutableMap.copyOf(extraData));
		}

		/**
		 * Returns a copy of this builder.  Subsequent changes to this builder
		 * have no effect on the copy, and vice versa.  This method is useful
		 * for creating "prototype" builders that can be cloned, customized,
		 * and built.
		 * @return a copy of this builder
		 */
		@Override
		public Builder clone() {
			//We're final, so we don't need to use super.clone().
			return new Builder(parameters, subconfigurations, extraData);
		}
	}

	/**
	 * Creates a new, empty builder.
	 * @return a new, empty builder
	 */
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Creates a new Builder with the same parameters, subconfigurations and
	 * extra data as the given configuration.
	 * @param config the Configuration to create a builder from
	 * @return a new Builder with the configuration's contents
	 */
	public static Builder builder(Configuration config) {
		return new Builder(config.getParametersMap(), config.getSubconfigurationsMap(), config.getExtraDataMap());
	}

	public static Configuration fromJson(String json) {
		return Jsonifiers.fromJson(json, Configuration.class);
	}

	public String toJson() {
		return Jsonifiers.toJson(this).toString();
	}

	/**
	 * JSON-ifies Configurations.  Note that Configuration handles its maps
	 * specially to simplify parsing on the Python side.
	 *
	 * This class is protected with a public constructor to allow ServiceLoader
	 * to instantiate it.
	 */
	@ServiceProvider(JsonifierFactory.class)
	protected static final class ConfigurationJsonifier implements Jsonifier<Configuration>, JsonifierFactory {
		public ConfigurationJsonifier() {}
		@Override
		public Configuration fromJson(JsonValue value) {
			JsonObject configObj = Jsonifiers.checkClassEqual(value, Configuration.class);
			JsonObject parametersObj = checkNotNull(configObj.getJsonObject("params"));
			JsonObject subconfigurationsObj = checkNotNull(configObj.getJsonObject("subconfigs"));
			JsonObject extraDataObj = checkNotNull(configObj.getJsonObject("extraData"));
			Builder builder = builder();
			for (Map.Entry<String, JsonValue> param : parametersObj.entrySet())
				builder.addParameter(Jsonifiers.fromJson(param.getValue(), Parameter.class));
			for (Map.Entry<String, JsonValue> subconfiguration : subconfigurationsObj.entrySet())
				builder.addSubconfiguration(subconfiguration.getKey(), Jsonifiers.fromJson(subconfiguration.getValue(), Configuration.class));
			for (Map.Entry<String, JsonValue> data : extraDataObj.entrySet()) {
				JsonArray arr = (JsonArray)data.getValue();
				Class<?> c = Jsonifiers.fromJson(arr.get(0), Class.class);
				builder.putExtraData(data.getKey(), Jsonifiers.fromJson(arr.get(1), c));
			}
			return builder.build();
		}

		@Override
		public JsonValue toJson(Configuration t) {
			JsonObjectBuilder paramsBuilder = Json.createObjectBuilder();
			for (Map.Entry<String, Parameter> param : t.parameters.entrySet())
				paramsBuilder.add(param.getKey(), Jsonifiers.toJson(param.getValue()));
			JsonObjectBuilder subconfigsBuilder = Json.createObjectBuilder();
			for (Map.Entry<String, Configuration> subconfig : t.subconfigurations.entrySet())
				subconfigsBuilder.add(subconfig.getKey(), Jsonifiers.toJson(subconfig.getValue()));
			JsonObjectBuilder extraDataBuilder = Json.createObjectBuilder();
			for (Map.Entry<String, Object> data : t.extraData.entrySet()) {
				JsonArrayBuilder da = Json.createArrayBuilder();
				da.add(Jsonifiers.toJson(data.getValue().getClass()));
				da.add(Jsonifiers.toJson(data.getValue()));
				extraDataBuilder.add(data.getKey(), da);
			}
			return Json.createObjectBuilder()
					.add("class", Jsonifiers.toJson(Configuration.class))
					.add("params", paramsBuilder)
					.add("subconfigs", subconfigsBuilder)
					.add("extraData", extraDataBuilder)
					//Python-side support
					.add("__module__", "configuration")
					.add("__class__", Configuration.class.getSimpleName())
					.build();
		}

		@Override
		@SuppressWarnings("unchecked")
		public <T> Jsonifier<T> getJsonifier(Class<T> klass) {
			return (Jsonifier<T>)(klass.equals(Configuration.class) ? this : null);
		}
	}

	/**
	 * Returns an immutable mapping of parameter names to the parameters in this
	 * configuration.
	 * @return an immutable mapping of the parameters in this configuration
	 */
	public ImmutableMap<String, Parameter> getParametersMap() {
		return parameters;
	}

	/**
	 * Gets the parameter with the given name, or null if this configuration
	 * doesn't contain a parameter with that name.
	 * @param name the name of the parameter
	 * @return the parameter, or null
	 */
	public Parameter getParameter(String name) {
		return parameters.get(checkNotNull(Strings.emptyToNull(name)));
	}

	/**
	 * Gets the parameter with the given name cast to the given parameter type,
	 * or null if this configuration doesn't contain a parameter with that name.
	 * If this configuration does have a parameter with that name but of a
	 * different type, a ClassCastException will be thrown.
	 * @param <T> the type of the parameter to get
	 * @param name the name of the parameter
	 * @param parameterType the type of the parameter
	 * @return the parameter, or null
	 * @throws ClassCastException if the parameter with the given name exists
	 * but is of a different type
	 */
	public <T extends Parameter> T getParameter(String name, Class<T> parameterType) {
		return checkNotNull(parameterType).cast(getParameter(name));
	}

	/**
	 * Gets the generic parameter with the given name cast to the given
	 * parameter type (including checking the type parameter type), or null if
	 * this configuration doesn't contain a parameter with that name.  If this
	 * configuration does have a parameter with that name but of a different
	 * type or with a different type parameter type,
	 */
	public <U, T extends GenericParameter<?>, V extends GenericParameter<U>> V getParameter(String name, Class<T> parameterType, Class<U> typeParameterType) {
		T parameter = getParameter(name, parameterType);
		if (parameter == null)
			return null;
		//This must be an exact match.
		if (parameter.getGenericParameter() != typeParameterType)
			throw new ClassCastException("Type parameter type mismatch: "+parameter.getGenericParameter() +" != "+typeParameterType);
		//Due to the checks above, this is safe.
		@SuppressWarnings("unchecked")
		V retval = (V)parameter;
		return retval;
	}

	/**
	 * Returns an immutable mapping of subconfiguration names to the
	 * subconfigurations of this configuration.
	 * @return an immutable mapping of the subconfigurations of this
	 * configuration
	 */
	public ImmutableMap<String, Configuration> getSubconfigurationsMap() {
		return subconfigurations;
	}

	/**
	 * Gets the subconfiguration with the given name, or null if this
	 * configuration doesn't contain a subconfiguration with that name.
	 * @param name the name of the subconfiguration
	 * @return the subconfiguration, or null
	 */
	public Configuration getSubconfiguration(String name) {
		return subconfigurations.get(checkNotNull(Strings.emptyToNull(name)));
	}

	/**
	 * Returns an immutable mapping of extra data names to the extra data of
	 * this configuration.
	 * @return an immutable mapping of the extra data of this configuration
	 */
	public ImmutableMap<String, Object> getExtraDataMap() {
		return extraData;
	}

	/**
	 * Gets the extra data with the given name, or null if this configuration
	 * doesn't contain extra data with that name
	 * @param name the name of the extra data
	 * @return the extra data, or null
	 */
	public Object getExtraData(String name) {
		return extraData.get(checkNotNull(Strings.emptyToNull(name)));
	}

	/**
	 * A Parameter is a configuration object with a name.  All implementations
	 * of this interface are immutable.
	 *
	 * Users of Configuration shouldn't implement this interface themselves;
	 * instead, use one of the provided implementations in Configuration.
	 */
	public interface Parameter extends java.io.Serializable {
		public String getName();
	}

	/**
	 * A GenericParameter is a Parameter with a type parameter. (The name
	 * GenericParameter was chosen in preference to ParameterizedParameter.)
	 *
	 * This interface isn't particularly interesting in and of itself; it mostly
	 * exists to make the Configuration.getParameter(String, Class<T>, Class<U>)
	 * overload have the proper (and checked) return type.
	 * @param <T>
	 */
	public interface GenericParameter<T> extends Parameter {
		public Class<?> getGenericParameter();
	}

	/**
	 * An IntParameter has an integer value that lies within some closed range.
	 * The lower and upper bounds are <b>inclusive</b>.
	 */
	public static final class IntParameter implements Parameter {
		private static final long serialVersionUID = 1L;
		private final String name;
		/**
		 * The Range of this IntParameter.  Note that this range is closed on
		 * both ends.
		 */
		private final Range<Integer> range;
		/**
		 * The value of this IntParameter, which must be contained in the range.
		 */
		private final int value;
		/**
		 * Constructs a new IntParameter.
		 * @param name the parameter's name
		 * @param min the minimum of the range (inclusive)
		 * @param max the maximum of the range (inclusive)
		 * @param value the parameter's value
		 */
		public IntParameter(String name, int min, int max, int value) {
			this(name, Range.closed(min, max), value);
		}
		/**
		 * Constructs a new IntParameter.
		 * @param name the parameter's name
		 * @param range the parameter's range, which must be nonempty and closed
		 * at both ends
		 * @param value the parameter's value
		 */
		public IntParameter(String name, Range<Integer> range, int value) {
			this.name = checkNotNull(Strings.emptyToNull(name));
			checkNotNull(range);
			checkArgument(range.hasLowerBound() && range.lowerBoundType() == BoundType.CLOSED
					&& range.hasUpperBound() && range.upperBoundType() == BoundType.CLOSED
					&& !range.isEmpty());
			this.range = range;
			checkArgument(range.contains(value), "value %s out of range %s", value, range);
			this.value = value;
		}

		@ServiceProvider(JsonifierFactory.class)
		protected static final class IntParameterJsonifier implements Jsonifier<IntParameter>, JsonifierFactory {
			public IntParameterJsonifier() {}
			@Override
			public IntParameter fromJson(JsonValue jsonvalue) {
				JsonObject obj = Jsonifiers.checkClassEqual(jsonvalue, IntParameter.class);
				String name = obj.getString("name");
				int min = obj.getInt("min");
				int max = obj.getInt("max");
				int value = obj.getInt("value");
				return new IntParameter(name, min, max, value);
			}

			@Override
			public JsonValue toJson(IntParameter t) {
				return Json.createObjectBuilder()
					.add("class", Jsonifiers.toJson(IntParameter.class))
					.add("name", t.getName())
					.add("min", t.getMin())
					.add("max", t.getMax())
					.add("value", t.getValue())
					//Python-side support
					.add("__module__", "sjparameters")
					.add("__class__", "sjIntegerParameter")
					.build();
			}

			@Override
			@SuppressWarnings("unchecked")
			public <T> Jsonifier<T> getJsonifier(Class<T> klass) {
				return (Jsonifier<T>)(klass.equals(IntParameter.class) ? this : null);
			}
		}
		@Override
		public String getName() {
			return name;
		}
		public int getMin() {
			return range.lowerEndpoint();
		}
		public int getMax() {
			return range.upperEndpoint();
		}
		public Range<Integer> getRange() {
			return range;
		}
		public int getValue() {
			return value;
		}
		@Override
		public boolean equals(Object obj) {
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			final IntParameter other = (IntParameter)obj;
			if (!Objects.equals(this.name, other.name))
				return false;
			if (!Objects.equals(this.range, other.range))
				return false;
			if (this.value != other.value)
				return false;
			return true;
		}
		@Override
		public int hashCode() {
			int hash = 3;
			hash = 97 * hash + Objects.hashCode(this.name);
			hash = 97 * hash + Objects.hashCode(this.range);
			hash = 97 * hash + this.value;
			return hash;
		}
		@Override
		public String toString() {
			return String.format("[%s: %d in %s]", name, value, range);
		}
	}

	public static final class FloatParameter implements Parameter {
		private static final long serialVersionUID = 1L;
		private final String name;
		private final Range<Float> range;
		private final float value;
		public FloatParameter(String name, float min, float max, float value) {
			this(name, Range.closed(min, max), value);
		}
		public FloatParameter(String name, Range<Float> range, float value) {
			this.name = checkNotNull(Strings.emptyToNull(name));
			checkNotNull(range);
			checkArgument(range.hasLowerBound() && range.lowerBoundType() == BoundType.CLOSED
					&& range.hasUpperBound() && range.upperBoundType() == BoundType.CLOSED
					&& !range.isEmpty());
			this.range = range;
			checkArgument(range.contains(value), "value %s out of range %s", value, range);
			this.value = value;
		}

		@ServiceProvider(JsonifierFactory.class)
		protected static final class FloatParameterJsonifier implements Jsonifier<FloatParameter>, JsonifierFactory {
			public FloatParameterJsonifier() {}
			@Override
			public FloatParameter fromJson(JsonValue jsonvalue) {
				JsonObject obj = Jsonifiers.checkClassEqual(jsonvalue, FloatParameter.class);
				String name = obj.getString("name");
				float min = obj.getJsonNumber("min").bigDecimalValue().floatValue();
				float max = obj.getJsonNumber("max").bigDecimalValue().floatValue();
				float value = obj.getJsonNumber("value").bigDecimalValue().floatValue();
				return new FloatParameter(name, min, max, value);
			}

			@Override
			public JsonValue toJson(FloatParameter t) {
				return Json.createObjectBuilder()
					.add("class", Jsonifiers.toJson(FloatParameter.class))
					.add("name", t.getName())
					.add("min", t.getMin())
					.add("max", t.getMax())
					.add("value", t.getValue())
					//Python-side support
					.add("__module__", "sjparameters")
					.add("__class__", "sjFloatParameter")
					.build();
			}

			@Override
			@SuppressWarnings("unchecked")
			public <T> Jsonifier<T> getJsonifier(Class<T> klass) {
				return (Jsonifier<T>)(klass.equals(FloatParameter.class) ? this : null);
			}
		}
		@Override
		public String getName() {
			return name;
		}
		public float getMin() {
			return range.lowerEndpoint();
		}
		public float getMax() {
			return range.upperEndpoint();
		}
		public Range<Float> getRange() {
			return range;
		}
		public float getValue() {
			return value;
		}
		@Override
		public boolean equals(Object obj) {
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			final FloatParameter other = (FloatParameter)obj;
			if (!Objects.equals(this.name, other.name))
				return false;
			if (!Objects.equals(this.range, other.range))
				return false;
			if (this.value != other.value)
				return false;
			return true;
		}
		@Override
		public int hashCode() {
			int hash = 3;
			hash = 97 * hash + Objects.hashCode(this.name);
			hash = 97 * hash + Objects.hashCode(this.range);
			hash = 97 * hash + Float.floatToIntBits(this.value);
			return hash;
		}
		@Override
		public String toString() {
			return String.format("[%s: %d in %s]", name, value, range);
		}
	}

	/**
	 * A SwitchParameter represents a choice of one of some universe of objects.
	 * For example, a SwitchParameter<Boolean> is a simple on-off flag, while a
	 * SwitchParameter<ChannelFactory> represents a choice of factories.
	 *
	 * The autotuner assumes there's no numeric relationship between the objects
	 * in the universe, in contrast to IntParameter, for which it will try to
	 * fit a model.
	 *
	 * The order of a SwitchParameter's universe is relevant for equals() and
	 * hashCode() and correct operation of the autotuner.  (To the autotuner, a
	 * SwitchParameter is just an integer between 0 and the universe size; if
	 * the order of the universe changes, the meaning of that integer changes
	 * and the autotuner will get confused.)
	 *
	 * Objects put into SwitchParameters must implements equals() and hashCode()
	 * for SwitchParameter's equals() and hashCode() methods to work correctly.
	 * Objects put into SwitchParameters must be immutable.
	 *
	 * To the extent possible, the type T should not itself contain type
	 * parameters.  Consider defining a new class or interface that fixes the
	 * type parameters.
	 *
	 * TODO: restrictions required for JSON representation: toString() and
	 * fromString/valueOf/String ctor, fallback to base64 encoded Serializable,
	 * etc; List/Set etc. not good unless contains only one type (e.g.,
	 * List<String> can be handled okay)
	 * @param <T>
	 */
	public static final class SwitchParameter<T> implements GenericParameter<T> {
		private static final long serialVersionUID = 1L;
		private final String name;
		/**
		 * The type of elements in this SwitchParameter.
		 */
		private final Class<T> type;
		/**
		 * The universe of this SwitchParameter -- must not contain any
		 * duplicate elements.
		 */
		private final ImmutableList<T> universe;
		/**
		 * The index of the value in the universe.  Note that most of the
		 * interface prefers to work with Ts rather than values.
		 */
		private final int value;

		/**
		 * Create a new SwitchParameter with the given type, value, and
		 * universe.  The universe must contain at least one element, contain no
		 * duplicate elements, and contain the value.
		 *
		 * The type must be provided explicitly, rather than being inferred as
		 * value.getClass(), as value might be of a more-derived type than the
		 * elements in the universe.
		 * @param name the name of this parameter
		 * @param type the type of the universe
		 * @param value the value of this parameter
		 * @param universe the universe of possible values of this parameter
		 */
		public SwitchParameter(String name, Class<T> type, T value, Iterable<? extends T> universe) {
			this.name = checkNotNull(Strings.emptyToNull(name));
			this.type = checkNotNull(type);
			int size = 0;
			ImmutableSet.Builder<T> builder = ImmutableSet.builder();
			for (T t : universe) {
				checkArgument(!ReflectionUtils.usesObjectEquality(t.getClass()), "all objects in universe must have proper equals()/hashCode()");
				builder.add(t);
				++size;
			}
			ImmutableSet<T> set = builder.build();
			checkArgument(set.size() == size, "universe contains duplicate elements");
			//A single element universe is permitted, through not particularly
			//useful.
			checkArgument(set.size() > 0, "empty universe");
			this.universe = set.asList();
			this.value = checkElementIndex(this.universe.indexOf(value), this.universe.size(), "value not in universe");
		}

		/**
		 * Creates a new SwitchParameter<Boolean> with the given name and value.
		 * The universe is [false, true].
		 * @param name the name of this parameter
		 * @param value the value of this parameter (true or false)
		 * @return a new SwitchParameter<Boolean> with the given name and value
		 */
		public static SwitchParameter<Boolean> create(String name, boolean value) {
			return new SwitchParameter<>(name, Boolean.class, value, Arrays.asList(false, true));
		}

		@ServiceProvider(JsonifierFactory.class)
		protected static final class SwitchParameterJsonifier implements Jsonifier<SwitchParameter<?>>, JsonifierFactory {
			public SwitchParameterJsonifier() {}
			@Override
			@SuppressWarnings({"unchecked", "rawtypes"})
			public SwitchParameter<?> fromJson(JsonValue jsonvalue) {
				JsonObject obj = Jsonifiers.checkClassEqual(jsonvalue, SwitchParameter.class);
				String name = obj.getString("name");
				Class<?> universeType = Jsonifiers.fromJson(obj.get("universeType"), Class.class);
				ImmutableList<?> universe = ImmutableList.copyOf(Jsonifiers.fromJson(obj.get("universe"), ReflectionUtils.getArrayType(universeType)));
				//We should have caught this in fromJson(v, universeType).
				assert Jsonifiers.notHeapPolluted(universe, universeType);
				int value = obj.getInt("value");
				return new SwitchParameter(name, universeType, universe.get(value), universe);
			}

			@Override
			public JsonValue toJson(SwitchParameter<?> t) {
				return Json.createObjectBuilder()
						.add("class", Jsonifiers.toJson(SwitchParameter.class))
						.add("name", t.getName())
						.add("universeType", Jsonifiers.toJson(t.type))
						.add("universe", Jsonifiers.toJson(t.universe.toArray()))
						.add("value", t.value)
						//Python-side support
						.add("__module__", "sjparameters")
						.add("__class__", "sjSwitchParameter")
						.build();
			}

			@Override
			@SuppressWarnings("unchecked")
			public <T> Jsonifier<T> getJsonifier(Class<T> klass) {
				return (Jsonifier<T>)(klass.equals(SwitchParameter.class) ? this : null);
			}
		}

		@Override
		public String getName() {
			return name;
		}

		@Override
		public Class<T> getGenericParameter() {
			return type;
		}

		/**
		 * Gets this parameter's value.
		 * @return this parameter's value
		 */
		public T getValue() {
			return universe.get(value);
		}

		/**
		 * Gets the universe of possible values for this parameter.
		 * @return the universe of possible values for this parameter
		 */
		public ImmutableList<T> getUniverse() {
			return universe;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			final SwitchParameter<?> other = (SwitchParameter<?>)obj;
			if (!Objects.equals(this.type, other.type))
				return false;
			if (!Objects.equals(this.universe, other.universe))
				return false;
			if (this.value != other.value)
				return false;
			return true;
		}

		@Override
		public int hashCode() {
			int hash = 7;
			hash = 47 * hash + Objects.hashCode(this.type);
			hash = 47 * hash + Objects.hashCode(this.universe);
			hash = 47 * hash + this.value;
			return hash;
		}

		@Override
		public String toString() {
			return String.format("[%s: %s (index %d) of %s]", name, getValue(), value, universe);
		}
	}

	public static final class PermutationParameter<T> implements GenericParameter<T> {
		private static final long serialVersionUID = 1L;
		private final String name;
		private final Class<T> universeType;
		private final ImmutableList<? extends T> universe;
		public PermutationParameter(String name, Class<T> universeType, Iterable<? extends T> universe) {
			this.name = checkNotNull(name);
			this.universeType = checkNotNull(universeType);
			this.universe = ImmutableList.copyOf(universe);
			for (T t : universe)
				checkArgument(universeType.isInstance(t), "%s not a %s", t, universeType);
		}
		@Override
		public String getName() {
			return name;
		}
		@Override
		public Class<?> getGenericParameter() {
			return universeType;
		}
		public ImmutableList<? extends T> getUniverse() {
			return universe;
		}
		@Override
		public boolean equals(Object obj) {
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			final PermutationParameter<?> other = (PermutationParameter<?>)obj;
			if (!Objects.equals(this.name, other.name))
				return false;
			if (!Objects.equals(this.universeType, other.universeType))
				return false;
			if (!Objects.equals(this.universe, other.universe))
				return false;
			return true;
		}
		@Override
		public int hashCode() {
			int hash = 3;
			hash = 79 * hash + Objects.hashCode(this.name);
			hash = 79 * hash + Objects.hashCode(this.universeType);
			hash = 79 * hash + Objects.hashCode(this.universe);
			return hash;
		}
		@Override
		public String toString() {
			return String.format("[%s: %s]", name, universe);
		}

		@ServiceProvider(JsonifierFactory.class)
		protected static final class PermutationParameterJsonifier implements Jsonifier<PermutationParameter<?>>, JsonifierFactory {
			public PermutationParameterJsonifier() {}
			@Override
			@SuppressWarnings({"unchecked", "rawtypes"})
			public PermutationParameter<?> fromJson(JsonValue value) {
				JsonObject obj = Jsonifiers.checkClassEqual(value, PermutationParameter.class);
				String name = obj.getString("name");
				Class<?> universeType = Jsonifiers.fromJson(obj.get("universeType"), Class.class);
				ImmutableList<?> universe = ImmutableList.copyOf(Jsonifiers.fromJson(obj.get("universe"), ReflectionUtils.getArrayType(universeType)));
				//We should have caught this in fromJson(v, universeType).
				assert Jsonifiers.notHeapPolluted(universe, universeType);
				return new PermutationParameter(name, universeType, universe);
			}
			@Override
			public JsonValue toJson(PermutationParameter<?> t) {
				return Json.createObjectBuilder()
						.add("class", Jsonifiers.toJson(PermutationParameter.class))
						.add("name", t.getName())
						.add("universeType", Jsonifiers.toJson(t.universeType))
						.add("universe", Jsonifiers.toJson(t.universe.toArray()))
						//Python-side support
						.add("__module__", "sjparameters")
						.add("__class__", "sjPermutationParameter")
						.build();
			}
			@Override
			@SuppressWarnings("unchecked")
			public <T> Jsonifier<T> getJsonifier(Class<T> klass) {
				return (Jsonifier<T>)(klass.equals(PermutationParameter.class) ? this : null);
			}
		}
	}

	/**
	 * A PartitionParameter represents a partitioning of a stream graph
	 * (workers) into Blobs, the kind of those Blobs, and the mapping of Blobs
	 * to cores on machines.
	 * <p/>
	 * For the purposes of this class, machines are considered distinct, but
	 * cores on the same machine are not.
	 */
	public static final class PartitionParameter implements Parameter {
		private static final long serialVersionUID = 1L;
		private final String name;

		/**
		 * Map of MachineID and the number of cores on corresponding machine. Always contains at least one
		 * element and all elements are always >= 1.
		 */
		private final ImmutableMap<Integer, Integer> machineCoreMap;

		/**
		 * A map of machineID and list of blobs on that machine. The inner
		 * lists are sorted.
		 */
		private final ImmutableMap<Integer, ImmutableList<BlobSpecifier>> machineBlobMap;
		/**
		 * The BlobFactories that can be used to create blobs. This list
		 * contains no duplicate elements.
		 */
		private final ImmutableList<BlobFactory> blobFactoryUniverse;
		/**
		 * The maximum identifier of a worker in the stream graph, used during
		 * deserialization to check that all workers have been assigned to a
		 * blob.
		 */
		private final int maxWorkerIdentifier;

		/**
		 * Only called by the builder.
		 */
		private PartitionParameter(String name, ImmutableMap<Integer, Integer> machineCoreMap, ImmutableMap<Integer, ImmutableList<BlobSpecifier>> blobs, ImmutableList<BlobFactory> blobFactoryUniverse, int maxWorkerIdentifier) {
			this.name = name;
			this.machineBlobMap = blobs;
			this.blobFactoryUniverse = blobFactoryUniverse;
			this.maxWorkerIdentifier = maxWorkerIdentifier;
			this.machineCoreMap = machineCoreMap;
		}

		public static final class Builder {
			private final String name;
			private final ImmutableMap<Integer, Integer> machineCoreMap;
			private final Map<Integer, Integer> coresAvailable;
			private final List<BlobFactory> blobFactoryUniverse = new ArrayList<>();
			private final Map<Integer, List<BlobSpecifier>> machineBlobMap = new HashMap<>();
			private final NavigableSet<Integer> workersInBlobs = new TreeSet<>();

			private Builder(String name, ImmutableMap<Integer, Integer> machineCoreMap) {
				this.name = name;
				this.machineCoreMap = machineCoreMap;
				this.coresAvailable = new HashMap<>(machineCoreMap);
				//You might think we can use Collections.nCopies() here, but
				//that would mean all cores would share the same list!
				for (int i : machineCoreMap.keySet())
					machineBlobMap.put(i, new ArrayList<BlobSpecifier>());
			}

			public Builder addBlobFactory(BlobFactory factory) {
				checkArgument(!ReflectionUtils.usesObjectEquality(factory.getClass()), "blob factories must have a proper equals() and hashCode()");
				checkArgument(!blobFactoryUniverse.contains(checkNotNull(factory)), "blob factory already added");
				blobFactoryUniverse.add(factory);
				return this;
			}

			public Builder addBlob(int machine, int cores, BlobFactory blobFactory, Set<Worker<?, ?>> workers) {
				checkArgument(machineCoreMap.containsKey(machine), "No machine with the machineID %d", machine);
				checkArgument(cores <= machineCoreMap.get(machine),
						"allocating %s cores but only %s available on machine %s",
						cores, machineCoreMap.get(machine), machine);
				checkArgument(blobFactoryUniverse.contains(blobFactory),
						"blob factory %s not in universe %s", blobFactory, blobFactoryUniverse);
				ImmutableSortedSet.Builder<Integer> builder = ImmutableSortedSet.naturalOrder();
				for (Worker<?, ?> worker : workers) {
					int identifier = Workers.getIdentifier(worker);
					checkArgument(identifier >= 0, "uninitialized worker identifier: %s", worker);
					checkArgument(!workersInBlobs.contains(identifier), "worker %s already assigned to blob", worker);
					builder.add(identifier);
				}
				ImmutableSortedSet<Integer> workerIdentifiers = builder.build();

				//Okay, we've checked everything.  Commit.
				machineBlobMap.get(machine).add(new BlobSpecifier(workerIdentifiers, machine, cores, blobFactory));
				workersInBlobs.addAll(workerIdentifiers);
				int remainingCores = coresAvailable.get(machine) - cores;
				coresAvailable.put(machine, remainingCores);
				return this;
			}

			public PartitionParameter build() {
				ImmutableMap.Builder<Integer, ImmutableList<BlobSpecifier>> blobBuilder = ImmutableMap.builder();
				for (Entry<Integer, List<BlobSpecifier>> blobMapEntry : machineBlobMap.entrySet()) {
					Collections.sort(blobMapEntry.getValue());
					blobBuilder.put(blobMapEntry.getKey(), ImmutableList.copyOf(blobMapEntry.getValue()));
				}
				return new PartitionParameter(name, machineCoreMap, blobBuilder.build(), ImmutableList.copyOf(blobFactoryUniverse), workersInBlobs.last());
			}
		}

		public static Builder builder(String name, Map<Integer, Integer> machineCoreMap) {
			checkArgument(!machineCoreMap.isEmpty());
			for (Integer i : machineCoreMap.values())
				checkArgument(checkNotNull(i) >= 1);
			return new Builder(checkNotNull(Strings.emptyToNull(name)), ImmutableMap.copyOf(machineCoreMap));
		}

		/**
		 * A blob's properties.
		 */
		public static final class BlobSpecifier implements Comparable<BlobSpecifier> {
			/**
			 * The identifiers of the workers in this blob.
			 */
			private final ImmutableSortedSet<Integer> workerIdentifiers;
			/**
			 * The index of the machine this blob is on.
			 */
			private final int machine;
			/**
			 * The number of cores allocated to this blob.
			 */
			private final int cores;
			/**
			 * The BlobFactory to be used to create this blob.
			 */
			private final BlobFactory blobFactory;

			private BlobSpecifier(ImmutableSortedSet<Integer> workerIdentifiers, int machine, int cores, BlobFactory blobFactory) {
				this.workerIdentifiers = workerIdentifiers;
				checkArgument(machine >= 0);
				this.machine = machine;
				checkArgument(cores >= 1, "all blobs must be assigned at least one core");
				this.cores = cores;
				this.blobFactory = blobFactory;
			}

			@ServiceProvider(JsonifierFactory.class)
			protected static final class BlobSpecifierJsonifier implements Jsonifier<BlobSpecifier>, JsonifierFactory {
				public BlobSpecifierJsonifier() {}
				@Override
				public BlobSpecifier fromJson(JsonValue value) {
					//TODO: array serialization, error checking
					JsonObject obj = Jsonifiers.checkClassEqual(value, BlobSpecifier.class);
					int machine = obj.getInt("machine");
					int cores = obj.getInt("cores");
					BlobFactory blobFactory = Jsonifiers.fromJson(obj.get("blobFactory"), BlobFactory.class);
					ImmutableSortedSet.Builder<Integer> builder = ImmutableSortedSet.naturalOrder();
					for (JsonValue i : obj.getJsonArray("workerIds"))
						builder.add(Jsonifiers.fromJson(i, Integer.class));
					return new BlobSpecifier(builder.build(), machine, cores, blobFactory);
				}
				@Override
				public JsonValue toJson(BlobSpecifier t) {
					JsonArrayBuilder workerIds = Json.createArrayBuilder();
					for (int i : t.workerIdentifiers)
						workerIds.add(i);
					return Json.createObjectBuilder()
							.add("class", Jsonifiers.toJson(BlobSpecifier.class))
							.add("machine", t.machine)
							.add("cores", t.cores)
							.add("blobFactory", Jsonifiers.toJson(t.blobFactory))
							.add("workerIds", workerIds)
							//Python-side support
							.add("__module__", "configuration")
							.add("__class__", BlobSpecifier.class.getSimpleName())
							.build();
				}
				@Override
				@SuppressWarnings("unchecked")
				public <T> Jsonifier<T> getJsonifier(Class<T> klass) {
					return (Jsonifier<T>)(klass.equals(BlobSpecifier.class) ? this : null);
				}
			}

			public ImmutableSortedSet<Integer> getWorkerIdentifiers() {
				return workerIdentifiers;
			}

			public ImmutableSet<Worker<?, ?>> getWorkers(Worker<?, ?> streamGraph) {
				ImmutableSet<Worker<?, ?>> allWorkers = Workers.getAllWorkersInGraph(streamGraph);
				ImmutableMap<Integer, Worker<?, ?>> workersByIdentifier =
						Maps.uniqueIndex(allWorkers, Workers::getIdentifier);
				ImmutableSet.Builder<Worker<?, ?>> workersInBlob = ImmutableSet.builder();
				for (Integer i : workerIdentifiers) {
					Worker<?, ?> w = workersByIdentifier.get(i);
					if (w == null)
						throw new IllegalArgumentException("Identifier " + i + " not in given stream graph");
					workersInBlob.add(w);
				}
				return workersInBlob.build();
			}

			public int getMachine() {
				return machine;
			}

			public int getCores() {
				return cores;
			}

			public BlobFactory getBlobFactory() {
				return blobFactory;
			}

			@Override
			public int hashCode() {
				int hash = 3;
				hash = 37 * hash + Objects.hashCode(this.workerIdentifiers);
				hash = 37 * hash + this.machine;
				hash = 37 * hash + this.cores;
				hash = 37 * hash + Objects.hashCode(this.blobFactory);
				return hash;
			}

			@Override
			public boolean equals(Object obj) {
				if (obj == null)
					return false;
				if (getClass() != obj.getClass())
					return false;
				final BlobSpecifier other = (BlobSpecifier)obj;
				if (!Objects.equals(this.workerIdentifiers, other.workerIdentifiers))
					return false;
				if (this.machine != other.machine)
					return false;
				if (this.cores != other.cores)
					return false;
				if (!Objects.equals(this.blobFactory, other.blobFactory))
					return false;
				return true;
			}

			@Override
			public int compareTo(BlobSpecifier o) {
				//Worker identifiers are unique within the stream graph, so
				//we can base our comparison on them.
				return workerIdentifiers.first().compareTo(o.workerIdentifiers.first());
			}
		}

		@ServiceProvider(JsonifierFactory.class)
		protected static final class PartitionParameterJsonifier implements Jsonifier<PartitionParameter>, JsonifierFactory {
			public PartitionParameterJsonifier() {}
			@Override
			public PartitionParameter fromJson(JsonValue value) {
				//TODO: array serialization, error checking
				JsonObject obj = Jsonifiers.checkClassEqual(value, PartitionParameter.class);
				String name = obj.getString("name");
				int maxWorkerIdentifier = obj.getInt("maxWorkerIdentifier");

				Map<Integer, Integer> machineCoreMap = new HashMap<>();
				JsonObject mapObj = checkNotNull(obj.getJsonObject("machineCoreMap"));
				for (Map.Entry<String, JsonValue> data : mapObj.entrySet()) {
					machineCoreMap.put(Integer.parseInt(data.getKey()), Jsonifiers.fromJson(data.getValue(), Integer.class));
				}

				ImmutableList.Builder<BlobFactory> blobFactoryUniverse = ImmutableList.builder();
				for (JsonValue v : obj.getJsonArray("blobFactoryUniverse"))
					blobFactoryUniverse.add(Jsonifiers.fromJson(v, BlobFactory.class));

				Map<Integer, List<BlobSpecifier>> blobMachineMap = new HashMap<>();
				JsonObject blobsObj = checkNotNull(obj.getJsonObject("machineBlobMap"));
				for (Map.Entry<String, JsonValue> data : blobsObj.entrySet()) {
					List<BlobSpecifier> bsList = new ArrayList<BlobSpecifier>();
					JsonArray arr = (JsonArray)data.getValue();
					for(int i = 0; i < arr.size(); i++)
					{
						BlobSpecifier bs = Jsonifiers.fromJson(arr.get(i), BlobSpecifier.class);
						if(bs.getMachine() != Integer.parseInt(data.getKey()))
							throw new IllegalArgumentException("fromJson error : Blobs and corresponding assigned machines mismatch");
						bsList.add(bs);
					}

					if(blobMachineMap.containsKey(Integer.parseInt(data.getKey())))
						throw new IllegalArgumentException("Multiple BlobSpecifier list exists for same machine");
					blobMachineMap.put(Integer.parseInt(data.getKey()), bsList);
				}

				ImmutableMap.Builder<Integer, ImmutableList<BlobSpecifier>> blobBuilder = ImmutableMap.builder();
				for (Entry<Integer, List<BlobSpecifier>> blobMapEntry : blobMachineMap.entrySet()) {
					Collections.sort(blobMapEntry.getValue());
					blobBuilder.put(blobMapEntry.getKey(), ImmutableList.copyOf(blobMapEntry.getValue()));
				}

				return new PartitionParameter(name, ImmutableMap.copyOf(machineCoreMap), blobBuilder.build(), blobFactoryUniverse.build(), maxWorkerIdentifier);
			}

			@Override
			public JsonValue toJson(PartitionParameter t) {

				JsonObjectBuilder machineCoreMapBuilder = Json.createObjectBuilder();
				for (Map.Entry<Integer, Integer> data : t.machineCoreMap.entrySet()) {
					machineCoreMapBuilder.add(data.getKey().toString(), Jsonifiers.toJson(data.getValue()));
				}

				JsonArrayBuilder blobFactoryUniverse = Json.createArrayBuilder();
				for (BlobFactory factory : t.blobFactoryUniverse)
					blobFactoryUniverse.add(Jsonifiers.toJson(factory));

				JsonObjectBuilder blobsBuilder = Json.createObjectBuilder();
				for (Map.Entry<Integer,ImmutableList<BlobSpecifier>> machine : t.machineBlobMap.entrySet()) {
					JsonArrayBuilder bsArraybuilder = Json.createArrayBuilder();
					for(BlobSpecifier bs : machine.getValue())
						bsArraybuilder.add(Jsonifiers.toJson(bs));
					blobsBuilder.add(machine.getKey().toString(), bsArraybuilder);
				}
				return Json.createObjectBuilder()
						.add("class", Jsonifiers.toJson(PartitionParameter.class))
						.add("name", t.getName())
						.add("maxWorkerIdentifier", t.maxWorkerIdentifier)
						.add("machineCoreMap", machineCoreMapBuilder)
						.add("blobFactoryUniverse", blobFactoryUniverse)
						.add("machineBlobMap", blobsBuilder)
						//Python-side support
						.add("__module__", "sjparameters")
						.add("__class__", "sjPartitionParameter")
						.build();
			}

			@Override
			@SuppressWarnings("unchecked")
			public <T> Jsonifier<T> getJsonifier(Class<T> klass) {
				return (Jsonifier<T>)(klass.equals(PartitionParameter.class) ? this : null);
			}
		}

		@Override
		public String getName() {
			return name;
		}

		public int getMachineCount() {
			return machineCoreMap.size();
		}

		public int getCoresOnMachine(int machine) {
			return machineCoreMap.get(machine);
		}

		public ImmutableList<BlobSpecifier> getBlobsOnMachine(int machine) {
			return machineBlobMap.get(machine);
		}

		public ImmutableList<BlobFactory> getBlobFactories() {
			return blobFactoryUniverse;
		}

		/**
		 * @param worker
		 * @return the machineID where on which the passed worker is assigned.
		 */
		public int getAssignedMachine(Worker<?, ?> worker) {
			int id = Workers.getIdentifier(worker);
			for(int machineID : machineBlobMap.keySet() )
			{
				for (BlobSpecifier bs : machineBlobMap.get(machineID)) {
					if (bs.getWorkerIdentifiers().contains(id))
						return machineID;
				}
			}
			throw new IllegalArgumentException(String.format("%s is not assigned to anyof the machines", worker));
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			final PartitionParameter other = (PartitionParameter)obj;
			if (!Objects.equals(this.name, other.name))
				return false;
			if (!Objects.equals(this.machineCoreMap, other.machineCoreMap))
				return false;
			if (!Objects.equals(this.machineBlobMap, other.machineBlobMap))
				return false;
			if (!Objects.equals(this.blobFactoryUniverse, other.blobFactoryUniverse))
				return false;
			if (this.maxWorkerIdentifier != other.maxWorkerIdentifier)
				return false;
			return true;
		}

		@Override
		public int hashCode() {
			int hash = 3;
			hash = 61 * hash + Objects.hashCode(this.name);
			hash = 61 * hash + Objects.hashCode(this.machineCoreMap);
			hash = 61 * hash + Objects.hashCode(this.machineBlobMap);
			hash = 61 * hash + Objects.hashCode(this.blobFactoryUniverse);
			hash = 61 * hash + this.maxWorkerIdentifier;
			return hash;
		}
	}

	/**
	 * Represents a composition of 1.0 into N addends (which may include 0),
	 * where the order is relevant.  (This isn't a mathematical composition, but
	 * it's close, and "PartitionParameter" was taken.  "SubdivisionParameter"?)
	 *
	 * Array jsonification is special-cased for Python's convenience.
	 */
	public static final class CompositionParameter implements Parameter {
		private static final long serialVersionUID = 1L;
		private final String name;
		private final double[] values;
		public CompositionParameter(String name, int length) {
			this.name = name;
			this.values = new double[length];
			Arrays.fill(this.values, 1.0/length);
		}
		private CompositionParameter(String name, double[] values) {
			this.name = name;
			this.values = values;
			double sum = 0;
			for (double d : values)
				sum += d;
			//check we're close, at least.
			if (!DoubleMath.fuzzyEquals(sum, 1, 0.00001))
				throw new IllegalArgumentException("sums to "+sum);
		}

		@ServiceProvider(JsonifierFactory.class)
		protected static final class CompositionParameterJsonifier implements Jsonifier<CompositionParameter>, JsonifierFactory {
			public CompositionParameterJsonifier() {}
			@Override
			public CompositionParameter fromJson(JsonValue jsonvalue) {
				JsonObject obj = Jsonifiers.checkClassEqual(jsonvalue, CompositionParameter.class);
				String name = obj.getString("name");
				JsonArray arr = obj.getJsonArray("values");
				double[] values = new double[arr.size()];
				for (int i = 0; i < arr.size(); ++i)
					values[i] = arr.getJsonNumber(i).doubleValue();
				return new CompositionParameter(name, values);
			}

			@Override
			public JsonValue toJson(CompositionParameter t) {
				JsonArrayBuilder arr = Json.createArrayBuilder();
				for (double d : t.values)
					arr.add(d);
				return Json.createObjectBuilder()
					.add("class", Jsonifiers.toJson(CompositionParameter.class))
					.add("name", t.getName())
					.add("values", arr)
					//Python-side support
					.add("__module__", "sjparameters")
					.add("__class__", "sjCompositionParameter")
					.build();
			}

			@Override
			@SuppressWarnings("unchecked")
			public <T> Jsonifier<T> getJsonifier(Class<T> klass) {
				return (Jsonifier<T>)(klass.equals(CompositionParameter.class) ? this : null);
			}
		}
		@Override
		public String getName() {
			return name;
		}
		public int getLength() {
			return values.length;
		}
		public double getValue(int slot) {
			return values[slot];
		}
		@Override
		public boolean equals(Object obj) {
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			final CompositionParameter other = (CompositionParameter)obj;
			if (!Objects.equals(this.name, other.name))
				return false;
			if (!Arrays.equals(this.values, other.values))
				return false;
			return true;
		}
		@Override
		public int hashCode() {
			int hash = 7;
			hash = 83 * hash + Objects.hashCode(this.name);
			hash = 83 * hash + Arrays.hashCode(this.values);
			return hash;
		}
		@Override
		public String toString() {
			return String.format("[%s: %s]", name, Arrays.toString(values));
		}
	}

	public static Configuration randomize(Configuration cfg, Random rng) {
		Configuration.Builder builder = Configuration.builder();
		for (Parameter p : cfg.getParametersMap().values()) {
			if (p instanceof IntParameter) {
				int min = ((IntParameter)p).getMin(), max = ((IntParameter)p).getMax();
				int newValue = rng.nextInt(max-min+1) + min;
				builder.addParameter(new IntParameter(p.getName(), min, max, newValue));
			} else if (p instanceof FloatParameter) {
				float min = ((FloatParameter)p).getMin(), max = ((FloatParameter)p).getMax();
				float newValue = rng.nextFloat() * (max-min) + min;
				assert min <= newValue && newValue <= max : min +" "+newValue+" "+max;
				builder.addParameter(new FloatParameter(p.getName(), min, max, newValue));
			} else if (p instanceof SwitchParameter) {
				SwitchParameter<?> sp = (SwitchParameter)p;
				builder.addParameter(new SwitchParameter(sp.getName(), sp.getGenericParameter(),
						sp.getUniverse().get(rng.nextInt(sp.getUniverse().size())), sp.getUniverse()));
			} else if (p instanceof PermutationParameter) {
				PermutationParameter<?> sp = (PermutationParameter)p;
				List<?> universe = new ArrayList<>(sp.getUniverse());
				Collections.shuffle(universe, rng);
				builder.addParameter(new PermutationParameter(sp.getName(), sp.getGenericParameter(), universe));
			} else
				throw new UnsupportedOperationException("don't know how to randomize a "+p.getClass()+" named "+p.getName());
		}
		for (Map.Entry<String, Object> e : cfg.getExtraDataMap().entrySet())
			builder.putExtraData(e.getKey(), e.getValue());
		return builder.build();
	}

	public static void main(String[] args) {
		Configuration.Builder builder = Configuration.builder();
		builder.addParameter(new IntParameter("foo", 0, 10, 8));
		builder.addParameter(SwitchParameter.create("bar", true));
		builder.addParameter(new SwitchParameter<>("baz", Integer.class, 3, Arrays.asList(1, 2, 3, 4)));
		builder.addParameter(new CompositionParameter("comp", 8));
		Configuration cfg = builder.build();
		String cfgJson = cfg.toJson();
		System.out.println(cfgJson);
		Configuration cfgrt = Configuration.fromJson(cfgJson);
		System.out.println(cfgrt);

		Identity<Integer> first = new Identity<>(), second = new Identity<>();
		Pipeline<Integer, Integer> pipeline = new Pipeline<>(first, second);
		ConnectWorkersVisitor cwv = new ConnectWorkersVisitor();
		pipeline.visit(cwv);

		Map<Integer, Integer> mapEx = new HashMap<>();
		mapEx.put(2, 8);
		mapEx.put(5, 16);
		mapEx.put(11, 24);
		mapEx.put(8, 12);
		mapEx.put(3, 32);
		mapEx.put(17, 64);

		List<Integer> crsPerMachine = new ArrayList<>();
		crsPerMachine.add(8);
		crsPerMachine.add(16);

		PartitionParameter.Builder partParam = PartitionParameter.builder("part", mapEx);
		BlobFactory factory = new Interpreter.InterpreterBlobFactory();
		partParam.addBlobFactory(factory);
		partParam.addBlob(17, 4, factory, Collections.<Worker<?, ?>>singleton(first));
		partParam.addBlob(17, 1, factory, Collections.<Worker<?, ?>>singleton(second));
		builder.addParameter(partParam.build());

		builder.putExtraData("one", 1);
		builder.putExtraData("two", 2);
		builder.putExtraData("topLevelClassName", first.getClass().getName());

		Configuration cfg1 = builder.build();
		SwitchParameter<Integer> parameter = cfg1.getParameter("baz", SwitchParameter.class, Integer.class);
		String json = Jsonifiers.toJson(cfg1).toString();
		//System.out.println(json);
		Configuration cfg2 = Jsonifiers.fromJson(json, Configuration.class);
		//System.out.println(cfg2);
		//String json2 = Jsonifiers.toJson(cfg2).toString();
		//System.out.println(json2);
		PartitionParameter partp = (PartitionParameter) cfg2.getParameter("part");

		System.out.println(partp.getCoresOnMachine(3));
		List<BlobSpecifier> blobList = partp.getBlobsOnMachine(17);

		for (BlobSpecifier bs : blobList)
			System.out.println(bs.getWorkerIdentifiers());

		//System.out.println(partp.getCoresOnMachineEx(17));

		/*Configuration.Builder builder = Configuration.builder();
		builder.addParameter(new IntParameter("foo", 0, 10, 8));
		Configuration cfg1 = builder.build();
		String json = Jsonifiers.toJson(cfg1).toString();
		System.out.println(json);*/

	}
}
