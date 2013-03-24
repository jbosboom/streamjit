package edu.mit.streamjit.impl.common;

import static com.google.common.base.Preconditions.*;
import com.google.common.base.Strings;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

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
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/23/2013
 */
public final class Configuration {
	private final ImmutableMap<String, Parameter> parameters;
	private final ImmutableMap<String, Configuration> subconfigurations;
	private Configuration(ImmutableMap<String, Parameter> parameters, ImmutableMap<String, Configuration> subconfigurations) {
		//We're only called by the builder, so assert, not throw IAE.
		assert parameters != null;
		assert subconfigurations != null;
		this.parameters = parameters;
		this.subconfigurations = subconfigurations;
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
		/**
		 * Constructs a new Builder.  Called only by Configuration.build().
		 */
		private Builder() {
			//Type inference fail.
			//These maps have their contents copied in the other constructor, so
			//just use these singleton empty maps.
			this(ImmutableMap.<String, Parameter>of(), ImmutableMap.<String, Configuration>of());
		}

		/**
		 * Constructs a new Builder with the given parameters and
		 * subconfigurations.  Called only by Builder.clone().
		 * @param parameters the parameters
		 * @param subconfigurations the subconfigurations
		 */
		private Builder(Map<String, Parameter> parameters, Map<String, Configuration> subconfigurations) {
			//Only called by our own code, so assert.
			assert parameters != null;
			assert subconfigurations != null;
			this.parameters = new HashMap<>(parameters);
			this.subconfigurations = new HashMap<>(subconfigurations);
		}

		public void addParameter(Parameter parameter) {
			checkNotNull(parameter);
			//The parameter constructor should enforce this, so assert.
			assert !Strings.isNullOrEmpty(parameter.getName()) : parameter;
			checkArgument(!parameters.containsKey(parameter.getName()), "conflicting names %s %s", parameters.get(parameter.getName()), parameters);
			parameters.put(parameter.getName(), parameter);
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

		public void addSubconfiguration(String name, Configuration subconfiguration) {
			checkNotNull(Strings.emptyToNull(name));
			checkNotNull(subconfiguration);
			checkArgument(!subconfigurations.containsKey(name), "name %s already in use", name);
			subconfigurations.put(name, subconfiguration);
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
		 * Builds a new Configuration from the parameters and subconfigurations
		 * added to this builder.  This builder is still valid and may be used
		 * to build more configurations (perhaps after adding or removing
		 * elements), but the returned configurations remain immutable.
		 * @return a new Configuration containing the parameters and
		 * subconfigurations added to this builder
		 */
		public Configuration build() {
			return new Configuration(ImmutableMap.copyOf(parameters), ImmutableMap.copyOf(subconfigurations));
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
			return new Builder(parameters, subconfigurations);
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
	 * A Parameter is a configuration object with a name.  All implementations
	 * of this interface are immutable.
	 *
	 * Users of Configuration shouldn't implement this interface themselves;
	 * instead, use one of the provided implementations in Configuration.
	 */
	public interface Parameter extends Serializable {
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
			checkArgument(range.contains(value));
			this.value = value;
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
	public static class SwitchParameter<T> implements GenericParameter<Boolean> {
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
				builder.add(t);
				++size;
			}
			ImmutableSet<T> set = builder.build();
			checkArgument(set.size() == size, "universe contains duplicate elements");
			//A single element universe is permitted, through not particularly
			//useful.
			checkArgument(set.size() == 0, "empty universe");
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
}
