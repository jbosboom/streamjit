package edu.mit.streamjit.impl.compiler;

import edu.mit.streamjit.util.IntrusiveList;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/3/2013
 */
public class Field implements ParentedList.Parented<Klass> {
	@IntrusiveList.Previous
	private Field previous;
	@IntrusiveList.Next
	private Field next;
	@ParentedList.Parent
	private Klass parent;
	@Override
	public Klass getParent() {
		return parent;
	}

}
