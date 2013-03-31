package edu.mit.streamjit.util;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.MapMaker;
import java.util.AbstractSequentialList;
import java.util.ConcurrentModificationException;
import java.util.ListIterator;
import java.util.concurrent.ConcurrentMap;
import java.util.NoSuchElementException;

/**
 * A List implementation using previous and next references embedded in the list
 * elements ("intrusive").  An immediate consequence of this structure is that
 * elements must have special support for IntrusiveList and may be in at most
 * one IntrusiveList at a time (though they may be in other lists).
 *
 * Instances of IntrusiveList contain elements of only one class (the class
 * containing the previous and next references) and its subclasses.  Note that
 * this class may be an interface type, if all implementations of the interface
 * have previous and next references of the interface type.  (This could be
 * avoided by introducing an IntrusiveListable interface, but that would make
 * the previous and next references public to all callers, not just
 * IntrusiveList; Java doesn't provide a way to limit access to interface
 * methods, even if the interface is itself private.)
 *
 * Each class wishing to participate in an IntrusiveList must register an
 * IntrusiveList.Support instance in its static initializer by calling
 * {@link #registerSupport(IntrusiveList.Support)}.  It is critical that the
 * Support instance is registered prior to (happens-before) creating any
 * IntrusiveLists for the supported class.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/30/2013
 */
public class IntrusiveList<T> extends AbstractSequentialList<T> {
	private final Support<T> support;
	private T head = null, tail = null;
	private int size = 0;
	public IntrusiveList(Class<T> klass) {
		checkNotNull(klass);
		Support<?> supportQ = SUPPORTS.get(klass);
		@SuppressWarnings("unchecked")
		Support<T> supportT = (Support<T>)supportQ;
		if (supportT == null)
			throw new UnsupportedOperationException("no Support for "+klass.getName());
		this.support = supportT;
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public ListIterator<T> listIterator(int index) {
		checkPositionIndex(index, size());
		T first;
		//Advance from whichever side of the list is shorter.
		if (index < size()/2) {
			first = head;
			for (int i = 0; i < index; ++i)
				first = support.getNext(first);
		} else if (index < size()) {
			first = tail;
			for (int i = size()-1; i > index; --i)
				first = support.getPrevious(first);
		} else { //index == size()
			first = null;
		}
		return new ListIter(first, index);
	}

	/**
	 * The ListIterator implementation.
	 */
	private class ListIter implements ListIterator<T> {
		private T next;
		private int nextIndex;
		private T lastReturned = null;
		private int expectedModCount = modCount;
		private ListIter(T next, int index) {
			this.next = next;
			this.nextIndex = index;
		}

		@Override
		public boolean hasNext() {
			return nextIndex() < size();
		}

		@Override
		public T next() {
			checkForComodification();
			if (!hasNext())
				throw new NoSuchElementException();
			lastReturned = next;
			next = support.getNext(next);
			++nextIndex;
			return lastReturned;
		}

		@Override
		public boolean hasPrevious() {
			return previousIndex() >= 0;
		}

		@Override
		public T previous() {
			checkForComodification();
			if (!hasPrevious())
				throw new NoSuchElementException();
			//If we're in the one-past-the-end position, return the tail.
			next = !hasNext() ? tail : support.getPrevious(next);
			lastReturned = next;
			--nextIndex;
			return lastReturned;
		}

		@Override
		public int nextIndex() {
			return nextIndex;
		}

		@Override
		public int previousIndex() {
			return nextIndex()-1;
		}

		@Override
		public void add(T t) {
			checkForComodification();
			checkNotNull(t); //can't put null in an intrusive list - no next/prev refs!
			checkArgument(!inList(t), "already in intrusive list: %s", t);

			T nextPrev;
			if (next == null) {
				nextPrev = tail;
				tail = t;
			} else
				nextPrev = support.setPrevious(next, t);
			support.setNext(t, next);
			support.setPrevious(t, nextPrev);
			if (nextPrev == null)
				head = t;
			else
				support.setNext(nextPrev, t);

			lastReturned = null;
			++nextIndex;
			++modCount; //linking is a structural modification
			++expectedModCount;

			elementAdded(t);
		}

		@Override
		public void remove() {
			checkForComodification();
			checkState(lastReturned != null);

			T lastReturnedPrev = support.setPrevious(lastReturned, null);
			T lastReturnedNext = support.setNext(lastReturned, null);
			if (lastReturnedPrev == null) {
				assert lastReturned == head;
				head = lastReturnedNext;
			} else
				support.setNext(lastReturnedPrev, lastReturnedNext);
			if (lastReturnedNext == null) {
				assert lastReturned == tail;
				tail = lastReturnedPrev;
			} else
				support.setPrevious(lastReturnedNext, lastReturnedPrev);

			T removedElement = lastReturned;
			if (next == lastReturned)
				next = lastReturnedNext;
			else
				--nextIndex;
			lastReturned = null;
			++modCount; //unlinking is a structural modification
			++expectedModCount;

			elementRemoved(removedElement);
		}

		@Override
		public void set(T t) {
			checkForComodification();
			checkState(lastReturned != null);

			remove();
			add(t);
		}

		private void checkForComodification() {
			if (modCount != expectedModCount)
				throw new ConcurrentModificationException();
		}
	}

	/**
	 * Called after the given element is added to this list.
	 * @param t an element added to this list
	 */
	protected void elementAdded(T t) {}

	/**
	 * Called after the given element is removed from this list.
	 * @param t an element removed from this list
	 */
	protected void elementRemoved(T t) {}

	/**
	 * Returns true if the given object is in an intrusive list (not necessarily
	 * this one), or false if it is not or if unsure.  That is, false negatives
	 * are tolerated but false positives are not.
	 *
	 * This implementation returns true if this object's previous or next
	 * reference is non-null and false otherwise.  This implementation thus has
	 * a false negative if the object is in a single-element list.
	 *
	 * Subclasses that can eliminate this false positive are encouraged to
	 * override this method.
	 * @param t an object that can be placed in an intrusive list (never null)
	 * @return true if the object is definitely in an intrusive list, false if
	 * it isn't or if unsure
	 */
	protected boolean inList(T t) {
		return support.getPrevious(t) != null || support.getNext(t) != null;
	}

	/**
	 * Support provides access to the next and previous references stored in a T
	 * instance without requiring them to be publicly accessible.
	 * @param <T> the type containing the next and previous references
	 */
	public static interface Support<T> {
		/**
		 * Gets the previous reference stored in the given object.
		 * @param t an object that can be contained in an intrusive list (never
		 * null)
		 * @return the given object's previous reference
		 */
		public T getPrevious(T t);
		/**
		 * Sets the previous reference stored in the given object, returning the
		 * old value.
		 * @param t an object that can be contained in an intrusive list (never
		 * null)
		 * @param newPrevious the new value of the previous reference (may be
		 * null)
		 * @return the old value of the given object's previous reference
		 */
		public T setPrevious(T t, T newPrevious);
		/**
		 * Gets the next reference stored in the given object.
		 * @param t an object that can be contained in an intrusive list (never
		 * null)
		 * @return the given object's next reference
		 */
		public T getNext(T t);
		/**
		 * Sets the next reference stored in the given object, returning the old
		 * value.
		 * @param t an object that can be contained in an intrusive list (never
		 * null)
		 * @param newNext the new value of the next reference (may be null)
		 * @return the old value of the given object's next reference
		 */
		public T setNext(T t, T newNext);
	}

	private static final ConcurrentMap<Class<?>, Support<?>> SUPPORTS = new MapMaker().concurrencyLevel(1).makeMap();
	/**
	 * Registers a Support for the given class.
	 * @param <T> the type being supported
	 * @param klass the class being supported
	 * @param support a Support for the given class
	 * @throws IllegalArgumentException if a Support has already been registered
	 * for the given class
	 * @throws NullPointerException if klass or support is null
	 */
	public static <T> void registerSupport(Class<T> klass, Support<T> support) {
		Support<?> previous = SUPPORTS.putIfAbsent(klass, support);
		if (previous != null)
			throw new IllegalArgumentException(String.format(
					"%s: registering %s, but %s already registered",
					klass, support, previous));
	}
	//Previously, I considered allowing more generic supports, but that would
	//require the IntrusiveList to perform additional type checks to ensure a
	//List<T> didn't have a ? super T sneak in.  I don't really need this
	//functionality right now so I'll put that off.  (You'd also have to widen
	//registerSupport to take a Support<? super T>.)
//	private static <T> Support<? super T> mostSpecificSupport(Class<T> klass) {
//		//Get all the Supports that are assignable from the given klass.
//		List<Support<? super T>> eligible = new ArrayList<>();
//		for (Support<?> supports : SUPPORTS.values())
//			if (supports.getSupportedClass().isAssignableFrom(klass)) {
//				//checked just above
//				@SuppressWarnings("unchecked")
//				Support<? super T> cast = (Support<? super T>)supports;
//				eligible.add(cast);
//			}
//		if (eligible.isEmpty())
//			throw new UnsupportedOperationException("no IntrusiveList.Support for "+klass);
//
//		boolean removed = true;
//		remove_loop: while (eligible.size() > 1 && removed) {
//			removed = false;
//			//For each support, check if its class is assignable to all eligible
//			//supports.  If so, it can be removed.  (We are assured that no two
//			//supports are for the same class because SUPPORTS is a Map.)
//			s: for (Iterator<Support<? super T>> it = eligible.iterator(); it.hasNext();) {
//				Support<? super T> s = it.next();
//				for (Support<? super T> t : eligible)
//					if (!t.getSupportedClass().isAssignableFrom(s.getSupportedClass()))
//						continue s;
//				it.remove();
//				removed = true;
//				continue remove_loop;
//			}
//		}
//		if (eligible.size() > 1)
//			throw new UnsupportedOperationException("ambiguous supports: "+eligible);
//		return eligible.get(0);
//	}
}
