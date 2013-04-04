package edu.mit.streamjit.util;

import static com.google.common.base.Preconditions.*;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.util.AbstractSequentialList;
import java.util.ConcurrentModificationException;
import java.util.ListIterator;

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
 * Each class wishing to participate in an IntrusiveList must annotate its
 * previous and next references with @Previous and @Next respectively.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/30/2013
 */
public class IntrusiveList<T> extends AbstractSequentialList<T> {
	private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
	private T head = null, tail = null;
	private int size = 0;
	private final MethodHandle mhGetPrevious, mhSetPrevious, mhGetNext, mhSetNext;
	public IntrusiveList(Class<T> klass) {
		checkNotNull(klass);
		Field previousField = null, nextField = null;
		for (Field f : klass.getDeclaredFields()) {
			if (f.isAnnotationPresent(Previous.class)) {
				checkArgument(previousField == null, "multiple previous fields on %s", klass);
				previousField = f;
			}
			if (f.isAnnotationPresent(Next.class)) {
				checkArgument(nextField == null, "multiple next fields on %s", klass);
				nextField = f;
			}
		}
		checkArgument(previousField != null, "no previous field on %s", klass);
		checkArgument(previousField.getType().equals(klass), "previous field on %s of wrong type", klass);
		checkArgument(nextField != null, "no next field on %s", klass);
		checkArgument(nextField.getType().equals(klass), "next field on %s of wrong type", klass);

		try {
			previousField.setAccessible(true);
			nextField.setAccessible(true);
			mhGetPrevious = LOOKUP.unreflectGetter(previousField);
			mhSetPrevious = LOOKUP.unreflectSetter(previousField);
			mhGetNext = LOOKUP.unreflectGetter(nextField);
			mhSetNext = LOOKUP.unreflectSetter(nextField);
		} catch (SecurityException | IllegalAccessException ex) {
			throw new IllegalArgumentException("error accessing %s fields", ex);
		}
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
				first = getNext(first);
		} else if (index < size()) {
			first = tail;
			for (int i = size()-1; i > index; --i)
				first = getPrevious(first);
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
			checkState(hasNext());
			lastReturned = next;
			next = getNext(next);
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
			checkState(hasPrevious());
			//If we're in the one-past-the-end position, return the tail.
			next = !hasNext() ? tail : getPrevious(next);
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
			elementAdding(t);

			T nextPrev;
			if (next == null) {
				nextPrev = tail;
				tail = t;
			} else
				nextPrev = setPrevious(next, t);
			setNext(t, next);
			setPrevious(t, nextPrev);
			if (nextPrev == null)
				head = t;
			else
				setNext(nextPrev, t);

			lastReturned = null;
			++nextIndex;
			++modCount; //linking is a structural modification
			++expectedModCount;
			++size;

			elementAdded(t);
		}

		@Override
		public void remove() {
			checkForComodification();
			checkState(lastReturned != null);
			elementRemoving(lastReturned);

			T lastReturnedPrev = setPrevious(lastReturned, null);
			T lastReturnedNext = setNext(lastReturned, null);
			if (lastReturnedPrev == null) {
				assert lastReturned == head;
				head = lastReturnedNext;
			} else
				setNext(lastReturnedPrev, lastReturnedNext);
			if (lastReturnedNext == null) {
				assert lastReturned == tail;
				tail = lastReturnedPrev;
			} else
				setPrevious(lastReturnedNext, lastReturnedPrev);

			T removedElement = lastReturned;
			if (next == lastReturned)
				next = lastReturnedNext;
			else
				--nextIndex;
			lastReturned = null;
			++modCount; //unlinking is a structural modification
			++expectedModCount;
			--size;

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
	 * Called before the given element is added to this list.
	 * @param t an element about to be added to this list
	 */
	protected void elementAdding(T t) {}

	/**
	 * Called after the given element is added to this list.
	 * @param t an element added to this list
	 */
	protected void elementAdded(T t) {}

	/**
	 * Called before the given element is removed from this list.
	 * @param t an element about to be removed from this list
	 */
	protected void elementRemoving(T t) {}

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
		return getPrevious(t) != null || getNext(t) != null;
	}

	private T getPrevious(T t) {
		try {
			return (T)mhGetPrevious.invoke(t);
		} catch (Throwable ex) {
			Thread.currentThread().stop(ex);
			throw new AssertionError("unreachable");
		}
	}
	private T setPrevious(T t, T newPrevious) {
		T oldPrevious = getPrevious(t);
		try {
			mhSetPrevious.invoke(t, newPrevious);
		} catch (Throwable ex) {
			Thread.currentThread().stop(ex);
		}
		return oldPrevious;
	}
	private T getNext(T t) {
		try {
			return (T)mhGetNext.invoke(t);
		} catch (Throwable ex) {
			Thread.currentThread().stop(ex);
			throw new AssertionError("unreachable");
		}
	}
	private T setNext(T t, T newNext) {
		T oldNext = getNext(t);
		try {
			mhSetNext.invoke(t, newNext);
		} catch (Throwable ex) {
			Thread.currentThread().stop(ex);
		}
		return oldNext;
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.FIELD)
	public @interface Next {}
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.FIELD)
	public @interface Previous {}

	public static void main(String[] args) {
		class Linkable {
			@Previous
			private Linkable previous;
			@Next
			private Linkable next;
		}

		IntrusiveList<Linkable> list = new IntrusiveList<>(Linkable.class);
		list.add(new Linkable());
		list.add(new Linkable());
		list.add(new Linkable());
		System.out.println(list);
	}
}
