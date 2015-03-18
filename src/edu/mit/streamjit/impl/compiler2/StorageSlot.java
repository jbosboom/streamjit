/*
 * Copyright (c) 2013-2015 Massachusetts Institute of Technology
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
public class StorageSlot {
	static final StorageSlot HOLE = new StorageSlot(null, -1), DUP = new StorageSlot(null, -2);
	private final Token token;
	private final int index;
	private StorageSlot(Token token, int index) {
		this.token = token;
		this.index = index;
	}
	public static StorageSlot live(Token token, int index) {
		assert token != null;
		assert index >= 0 : index;
		return new StorageSlot(token, index);
	}
	public static StorageSlot hole() {
		return StorageSlot.HOLE;
	}

	public boolean isHole() {
		return this == HOLE;
	}
	public boolean isDuplicate() {
		return this == DUP;
	}
	public boolean isLive() {
		return this != HOLE;
	}
	public boolean isDrainable() {
		//return this != HOLE && this != DUP
		return token != null;
	}
	public StorageSlot duplify() {
		return DUP;
	}
	public Token token() {
		assert isDrainable();
		return token;
	}
	public int index() {
		assert isDrainable();
		return index;
	}
	@Override
	public String toString() {
		if (isHole()) return "(hole)";
		if (this == DUP) return "(dup)";
		return String.format("%s[%d]", token(), index());
	}
}
