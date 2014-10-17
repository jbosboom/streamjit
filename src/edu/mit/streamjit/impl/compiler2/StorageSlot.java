/*
 * Copyright (c) 2013-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package edu.mit.streamjit.impl.compiler2;

import edu.mit.streamjit.impl.blob.Blob.Token;

/**
 * A StorageSlot represents a slot in a storage: whether it's live or not, and
 * if it is, where it should go when we drain.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
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
			return DuplicateStorageSlot.INSTANCE;
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

	private static final class DuplicateStorageSlot extends StorageSlot {
		private static final DuplicateStorageSlot INSTANCE = new DuplicateStorageSlot();
		private DuplicateStorageSlot() {}
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
			return false;
		}
		@Override
		public StorageSlot duplify() {
			return this;
		}
		@Override
		public Token token() {
			throw new AssertionError("called token() on a duplicate");
		}
		@Override
		public int index() {
			throw new AssertionError("called index() on a duplicate");
		}
		@Override
		public String toString() {
			return String.format("(dup)");
		}
	}

	protected StorageSlot() {}
}
