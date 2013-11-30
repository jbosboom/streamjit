package edu.mit.streamjit.impl.compiler2;

import edu.mit.streamjit.impl.blob.Blob.Token;

/**
 * A StorageSlot represents a slot in a storage: whether it's live or not, and
 * if it is, where it should go when we drain.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/29/2013
 */
public abstract class StorageSlot {
	public static StorageSlot live(Token token, int index) {
		assert token != null;
		assert index >= 0 : index;
		return new LiveStorageSlot(token, index);
	}
	public static StorageSlot hole() {
		return HoleStorageSlot.INSTANCE;
	}

	public abstract boolean isLive();
	public abstract boolean isHole();
	public abstract boolean isDrainable();
	public abstract StorageSlot duplify();
	public abstract Token token();
	public abstract int index();
	@Override
	public abstract String toString();

	private static final class HoleStorageSlot extends StorageSlot {
		private static final HoleStorageSlot INSTANCE = new HoleStorageSlot();
		private HoleStorageSlot() {}
		@Override
		public boolean isLive() {
			return false;
		}
		@Override
		public boolean isHole() {
			return true;
		}
		@Override
		public boolean isDrainable() {
			return false;
		}
		@Override
		public StorageSlot duplify() {
			return this; //a hole duplicate is just a hole
		}
		@Override
		public Token token() {
			throw new AssertionError("called token() on a hole");
		}
		@Override
		public int index() {
			throw new AssertionError("called index() on a hole");
		}
		@Override
		public String toString() {
			return "(hole)";
		}
	}

	private static class LiveStorageSlot extends StorageSlot {
		private final Token token;
		private final int index;
		/**
		 * If we've duplified, the duplicate slot we created.  Caching this
		 * prevents a duplicate explosion.
		 */
		private StorageSlot duplicate;
		protected LiveStorageSlot(Token token, int index) {
			this.token = token;
			this.index = index;
		}
		@Override
		public boolean isLive() {
			return true;
		}
		@Override
		public boolean isHole() {
			return false;
		}
		@Override
		public boolean isDrainable() {
			return true;
		}
		@Override
		public StorageSlot duplify() {
			if (duplicate == null)
				duplicate = new DuplicateStorageSlot(token(), index());
			return duplicate;
		}
		@Override
		public Token token() {
			return token;
		}
		@Override
		public int index() {
			return index;
		}
		@Override
		public String toString() {
			return String.format("%s[%d]", token(), index());
		}
	}

	private static final class DuplicateStorageSlot extends LiveStorageSlot {
		private DuplicateStorageSlot(Token token, int index) {
			super(token, index);
		}
		@Override
		public boolean isDrainable() {
			return false;
		}
		@Override
		public StorageSlot duplify() {
			return this;
		}
		@Override
		public String toString() {
			return String.format("dup:%s[%d]", token(), index());
		}
	}

	protected StorageSlot() {}
}
