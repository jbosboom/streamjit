package edu.mit.streamjit.impl.compiler;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Value is the base class of all IR constructs that can be operands of other
 * Values.  Value maintains a list of all its uses. Values may also have a name,
 * but names have no semantic significance in the IR; the object identity of the
 * value is its identity.  However, the names may be used when emitting
 * bytecode, and some classes will enforce unique names to avoid collisions.
 *
 * All Values have a Type, which cannot change during the lifetime of the Value.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/6/2013
 */
public abstract class Value {
	private final Type type;
	private String name;
	//TODO: most Values won't have many uses, so consider a list-backed set?
	private final Set<Use> uses = new HashSet<>();
	public Value(Type type) {
		this.type = type;
	}
	public Value(Type type, String name) {
		this.type = type;
		this.name = name;
	}

	public Type getType() {
		return type;
	}

	/**
	 * Gets this Value's name, which may be null.
	 * @return this Value's name, or null
	 */
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void addUse(Use use) {
		assert use.getOperand() == this : "Adding use of wrong object"+use+", "+this;
		boolean added = uses.add(use);
		assert added : "Adding duplicate use: " + use;
	}

	public void removeUse(Use use) {
		assert use.getOperand() == this : "Removing use of wrong object"+use+", "+this;
		boolean removed = uses.remove(use);
		assert removed : "Removing not-a-use use: " + use;
	}

	public int getUseCount() {
		return uses.size();
	}

	public Iterator<Use> useIterator() {
		return uses.iterator();
	}
}
