package edu.mit.streamjit.impl.compiler;

import edu.mit.streamjit.util.IntrusiveList;

/**
 * A Klass the the IR node representing a class, interface or primitive type;
 * basically, it's a symbolic representation of a Class object.
 * (The name "Class" was already taken.)
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/3/2013
 */
public class Klass implements ParentedList.Parented<Module> {
	@IntrusiveList.Next
	private Klass next;
	@IntrusiveList.Previous
	private Klass previous;
	@ParentedList.Parent
	private Module parent;
	@Override
	public Module getParent() {
		return parent;
	}

}
