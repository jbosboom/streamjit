package edu.mit.streamjit.util;

import static com.google.common.base.Preconditions.*;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

/**
 * A ParentedList is an intrusive list of objects with parents that must be kept
 * in synch as objects are added and removed from the list. Only one instance of
 * ParentedList per element type should be created per parent instance, or the
 * instances may get confused as to which elements are in which list.
 * <p/>
 * Each class wishing to participate in a ParentedList must annotate their
 * parent field with @Parent.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/30/2013
 */
public class ParentedList<P, C extends Parented<P>> extends IntrusiveList<C> {
	private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
	private final P parent;
	private final MethodHandle mhSetParent;
	public ParentedList(P parent, Class<C> klass) {
		super(klass);
		this.parent = checkNotNull(parent);
		java.lang.reflect.Field parentField = null;
		for (java.lang.reflect.Field f : klass.getDeclaredFields())
			if (f.isAnnotationPresent(Parent.class)) {
				checkArgument(parentField == null, "two parent fields in %s", klass);
				parentField = f;
			}
		checkArgument(parentField != null, "no parent field in %s", klass);
		checkArgument(parentField.getType().isAssignableFrom(parent.getClass()), "parent field on %s of wrong type", klass);

		try {
			parentField.setAccessible(true);
			mhSetParent = LOOKUP.unreflectSetter(parentField);
		} catch (SecurityException | IllegalAccessException ex) {
			throw new IllegalArgumentException("error accessing %s field", ex);
		}
	}

	@Override
	protected void elementAdded(C t) {
		assert t.getParent() == null;
		setParent(t, parent);
	}

	@Override
	protected void elementRemoved(C t) {
		assert t.getParent() == parent;
		setParent(t, null);
	}

	/**
	 * Returns true iff the object has a parent or if it's in an IntrusiveList
	 * (by IntrusiveList.inList()).
	 * @param t {@inheritDoc}
	 * @return {@inheritDoc}
	 */
	@Override
	protected boolean inList(C t) {
		//If it's in a ParentedList, it has a parent.  Otherwise just do the
		//normal check for any other IntrusiveLists (?) it might be in.
		return t.getParent() != null || super.inList(t);
	}

	@Override
	public boolean contains(Object o) {
		//Assuming we're maintaining parents correctly, we own an object if it
		//has our parent, letting us skip the traversal.
		boolean parentCheck = o instanceof Parented<?> && ((Parented<?>)o).getParent() == parent;
		//Make sure we agree with the list walk.
		assert parentCheck == super.contains(o);
		return parentCheck;
	}

	private void setParent(C t, P newParent) {
		try {
			mhSetParent.invoke(t, newParent);
		} catch (Throwable ex) {
			Thread.currentThread().stop(ex);
		}
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.FIELD)
	public @interface Parent {}
}
