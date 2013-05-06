package edu.mit.streamjit.util;

import java.util.Objects;

/**
 * An immutable pair of references.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/30/2013
 */
public final class Pair<A, B> {
	public final A first;
	public final B second;
	public Pair(A first, B second) {
		this.first = first;
		this.second = second;
	}

	/**
	 * Construct a new Pair holding the two object references.  This method
	 * exists to take advantage of type inference.
	 * @param <A> the type of the first element
	 * @param <B> the type of the second element
	 * @param first the first element
	 * @param second the second element
	 * @return a new Pair containing the two elements
	 */
	public static <A, B> Pair<A, B> make(A first, B second) {
		return new Pair<>(first, second);
	}

	public <X> Pair<X, B> withFirst(X first) {
		return new Pair<>(first, second);
	}
	public <X> Pair<A, X> withSecond(X second) {
		return new Pair<>(first, second);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		final Pair<?, ?> other = (Pair<?, ?>)obj;
		if (!Objects.equals(this.first, other.first))
			return false;
		if (!Objects.equals(this.second, other.second))
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		int hash = 5;
		hash = 67 * hash + Objects.hashCode(this.first);
		hash = 67 * hash + Objects.hashCode(this.second);
		return hash;
	}

	@Override
	public String toString() {
		return String.format("(%s, %s)", first, second);
	}
}
