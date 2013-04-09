package edu.mit.streamjit.impl.compiler;

import edu.mit.streamjit.impl.compiler.types.Type;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import edu.mit.streamjit.util.ReflectionUtils;
import java.util.Objects;

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
	/**
	 * The set of uses of this value.  ImmutableSet serves two purposes here:
	 * 1) most values have few uses, and ImmutableSet stores them compactly and
	 * in a cheap-to-iterate form; 2) we can cheaply return an unchanging set
	 * from uses(), sparing the caller the question of whether they directly
	 * or indirectly modify it (and thus need to make a copy to iterate over).
	 */
	private ImmutableSet<Use> uses = ImmutableSet.of();
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

	/**
	 * Registers a use of this Value.  Should only be called by Use itself.
	 * @param use the use to add
	 */
	void addUse(Use use) {
		//We assert rather than check because this is for internal use only.
		assert use != null;
		assert ReflectionUtils.calledDirectlyFrom(Use.class);
		assert use.getOperand() == this : "Adding use of wrong object"+use+", "+this;
		ImmutableSet<Use> newUses = ImmutableSet.<Use>builder().addAll(uses).add(use).build();
		assert newUses.size() == uses.size()+1 : "Adding duplicate use: " + use;
		this.uses = newUses;
	}

	/**
	 * Unregisters a use of this Value.  Should only be called by Use itself.
	 * @param use the use to remove
	 */
	void removeUse(Use use) {
		assert use != null;
		assert ReflectionUtils.calledDirectlyFrom(Use.class);
		assert use.getOperand() == this : "Removing use of wrong object"+use+", "+this;
		ImmutableSet.Builder<Use> builder = ImmutableSet.builder();
		for (Use u : uses)
			if (!Objects.equals(u, use))
				builder.add(u);
		ImmutableSet<Use> newUses = builder.build();
		assert newUses.size() == uses.size()-1 : "Removing not-a-use use: " + use;
		this.uses = newUses;
	}

	/**
	 * Returns an immutable set of the uses of this value. Note that a User may
	 * use this value more than once, which is represented by multiple Use
	 * objects.
	 * <p/>
	 * The returned set will not change even if uses are added to or removed
	 * from this value, so it is safe to iterate over even if the loop body may
	 * change this value's use set.
	 * @return an immutable set of this value's uses
	 */
	public ImmutableSet<Use> uses() {
		return uses;
	}

	/**
	 * Returns an immutable multiset of the users of this value. Note that a User may
	 * use this value more than once, in which case it will appear that many times
	 * in the multiset. To iterate over each user only once, call Multiset.elementSet()
	 * on the returned multiset.
	 * <p/>
	 * The returned set will not change even if uses are added to or removed
	 * from this value, so it is safe to iterate over even if the loop body may
	 * change this value's use set.
	 * @return an immutable multiset of this value's users
	 */
	public ImmutableMultiset<User> users() {
		//If this method is called often, we can cache it in a field to avoid
		//building many copies.  Calls to add/removeUse() would set the field to
		//null to invalidate it and we'd rebuild in users() when required.
		ImmutableMultiset.Builder<User> users = ImmutableMultiset.builder();
		for (Use u : uses())
			users.add(u.getUser());
		return users.build();
	}

	/**
	 * Replaces all uses of this value with the given value.  Unlike LLVM, we do
	 * not require that the other value be the same type, but Users will perform
	 * their own checks and may well reject the reassignment.
	 *
	 * If this method returns (rather than throwing an exception), this value
	 * will have no uses.
	 * @param value the value to replace this value with
	 */
	public void replaceAllUsesWith(Value value) {
		for (Use use : uses())
			use.setOperand(value);
	}
}
