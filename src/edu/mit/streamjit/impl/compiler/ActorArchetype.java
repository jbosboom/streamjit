package edu.mit.streamjit.impl.compiler;

import com.google.common.collect.ImmutableList;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.StatefulFilter;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.util.ReflectionUtils;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * Contains information about a Worker subclass, detached from any particular
 * instance.  ActorArchetype's primary purpose is to hold the information
 * necessary to create common work functions for a particular worker class, with
 * method arguments for its channels and fields.  These methods may later be
 * specialized if desired through the usual inlining mechanisms.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 9/20/2013
 */
public class ActorArchetype {
	private final Class<? extends Worker<?, ?>> workerClass;
	/**
	 * The worker's nonstatic fields, final and nonfinal, including inherited
	 * fields up to but not including Filter, Splitter or Joiner (i.e., only
	 * user code fields).
	 *
	 * These are the fields that must be passed to a work method instance.
	 */
	private final ImmutableList<Field> fields;

	public ActorArchetype(Class<? extends Worker<?, ?>> workerClass) {
		this.workerClass = workerClass;

		ImmutableList.Builder<Field> fieldsBuilder = ImmutableList.builder();
		for (Class<?> c = this.workerClass; c != Filter.class && c != Splitter.class && c != Joiner.class; c = c.getSuperclass()) {
			for (Field f : c.getDeclaredFields())
				if (!Modifier.isStatic(f.getModifiers()))
					fieldsBuilder.add(f);
		}
		this.fields = fieldsBuilder.build();
	}

	public Class<? extends Worker<?, ?>> workerClass() {
		return workerClass;
	}

	public ImmutableList<Field> fields() {
		return fields;
	}

	public Class<?> inputType() {
		//TODO
		return Object.class;
	}

	public Class<?> outputType() {
		//TODO
		return Object.class;
	}

	public boolean isStateful() {
		return ReflectionUtils.getAllSupertypes(workerClass()).contains(StatefulFilter.class);
	}
}
