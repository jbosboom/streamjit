package edu.mit.streamjit.impl.compiler2;

import edu.mit.streamjit.impl.blob.Blob.Token;

/**
 * A StorageSlot represents a slot in a storage: whether it's live or not, and
 * if it is, where it should go when we drain.
 *
 * TODO: we could use three subclasses here to allow removing the type field
 * (embedding its information in the class).
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/29/2013
 */
public final class StorageSlot {
	public static enum Type {
		/**
		 * A live item that will be read during draining.  The token and index
		 * fields of this StorageSlot are valid.
		 */
		LIVE,
		/**
		 * A live item that won't be read during draining because it's a
		 * duplicate of another live item.  The token and index
		 * fields of this StorageSlot are valid, though they're only useful for
		 * assertions.
		 */
		LIVE_DUPLICATE,
		/**
		 * A slot that doesn't contain a live item at the beginning of a
		 * steady-state iteration.  Note that holes may be temporarily occupied
		 * during an iteration.  The token and index fields of this StorageSlot
		 * are not valid.
		 */
		HOLE
	}
	/**
	 * This StorageSlot's type.
	 */
	private final Type type;
	/**
	 * If this StorageSlot is LIVE or LIVE_DUPLICATE, the Token onto which we'll
	 * drain the data from this slot.
	 */
	private final Token token;
	/**
	 * If this StorageSlot is LIVE or LIVE_DUPLICATE, the index of this slot in
	 * the Token we're draining to.
	 */
	private final int index;
	private StorageSlot(Type type, Token token, int index) {
		this.type = type;
		this.token = token;
		this.index = index;
	}
	public static StorageSlot live(Token token, int index) {
		assert token != null;
		assert index >= 0 : index;
		return new StorageSlot(Type.LIVE, token, index);
	}
	private static final StorageSlot HOLE_SINGLETON = new StorageSlot(Type.HOLE, null, -1);
	public static StorageSlot hole() {
		return HOLE_SINGLETON;
	}

	public boolean isLive() {
		return type == Type.LIVE || type == Type.LIVE_DUPLICATE;
	}

	public boolean isHole() {
		return type == Type.HOLE;
	}

	public boolean isDrainable() {
		return type == Type.LIVE;
	}

	public StorageSlot duplify() {
		switch (type) {
			case HOLE: //duplicate holes are still holes
			case LIVE_DUPLICATE: //already a duplicate
				return this;
			case LIVE:
				return new StorageSlot(Type.LIVE_DUPLICATE, token, index);
		}
		throw new AssertionError("unreachable");
	}

	public Token token() {
		assert type != Type.HOLE;
		return token;
	}

	public int index() {
		assert type != Type.HOLE;
		return index;
	}

	@Override
	public String toString() {
		if (type == Type.HOLE)
			return type.toString();
		return String.format("%s: %s@%d", type, token, index);
	}
}
