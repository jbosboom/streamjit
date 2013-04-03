package edu.mit.streamjit.impl.compiler;

import java.util.ArrayList;
import java.util.List;

/**
 * Module is the top-level IR node for a single compilation, analogous to a
 * translation unit in other compilers.  It contains Methods, (static) Fields,
 * and (eventually) class definitions.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/6/2013
 */
public class Module {
	private final ParentedList<Module, Method> methods = new ParentedList<>(this, Method.class);
	/**
	 * Creates an empty Module with no contents.
	 */
	public Module() {
	}

	public List<Method> methods() {
		return methods;
	}
}
