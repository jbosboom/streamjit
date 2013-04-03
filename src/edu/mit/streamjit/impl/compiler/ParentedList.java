package edu.mit.streamjit.impl.compiler;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.MapMaker;
import edu.mit.streamjit.impl.compiler.ParentedList.Parented;
import edu.mit.streamjit.util.IntrusiveList;
import java.util.concurrent.ConcurrentMap;

/**
 * A ParentedList is an intrusive list of objects with parents that must be kept
 * in synch as objects are added and removed from the list. Only one instance of
 * ParentedList per element type should be created per parent instance, or the
 * instances may get confused as to which elements are in which list.
 * <p/>
 * Each class wishing to participate in a ParentedList must register a
 * ParentedList.Support instance in its static initializer by calling
 * {@link #registerSupport(ParentedList.Support)}. It is critical that the
 * Support instance is registered prior to (happens-before) creating any
 * ParentedLists for the supported class.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/30/2013
 */
public class ParentedList<P, C extends Parented<P>> extends IntrusiveList<C> {
	private final Support<P, C> support;
	private final P parent;
	public ParentedList(P parent, Class<C> klass) {
		super(klass);
		Support<?, ?> supportQ = SUPPORTS.get(klass);
		if (supportQ == null)
			throw new UnsupportedOperationException("no support for class: "+klass);
		@SuppressWarnings("unchecked")
		Support<P, C> supportPC = (Support<P, C>)supportQ;
		this.support = supportPC;
		this.parent = checkNotNull(parent);
	}

	@Override
	protected void elementAdded(C t) {
		P oldParent = support.setParent(t, parent);
		assert oldParent == null;
	}

	@Override
	protected void elementRemoved(C t) {
		P oldParent = support.setParent(t, null);
		assert oldParent == parent;
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

	public static interface Parented<P> {
		public P getParent();
	}

	/**
	 * Support provides a method for setting a Parented object's parent
	 * reference without requiring it to be publicly available, in addition to
	 * the methods on IntrusiveList.Support.
	 * @param <P> the parent's type
	 * @param <T> the Parented's type
	 */
	public static interface Support<P, T extends Parented<P>> extends IntrusiveList.Support<T> {
		/**
		 * Sets the given Parented's parent, returning the previous parent.
		 * @param t a Parented object (never null)
		 * @param newParent the new parent (may be null)
		 * @return the old parent
		 */
		public P setParent(T t, P newParent);
	}

	private static final ConcurrentMap<Class<?>, Support<?, ?>> SUPPORTS = new MapMaker().concurrencyLevel(1).makeMap();
	/**
	 * Registers a Support for the given class.  Also registers the Support with
	 * IntrusiveList.
	 * @param <P> the parent's type
	 * @param <T> the type being supported
	 * @param klass the class being supported
	 * @param support a Support for the given class
	 * @throws IllegalArgumentException if a Support has already been registered
	 * for the given class
	 * @throws NullPointerException if klass or support is null
	 */
	public static <P, T extends Parented<P>> void registerSupport(Class<T> klass, Support<P, T> support) {
		IntrusiveList.registerSupport(klass, support);
		Support<?, ?> previous = SUPPORTS.putIfAbsent(klass, support);
		if (previous != null)
			throw new IllegalArgumentException(String.format(
					"%s: registering %s, but %s already registered",
					klass, support, previous));
	}
}
