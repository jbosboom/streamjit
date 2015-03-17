/*
 * Copyright (c) 2015 Massachusetts Institute of Technology
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
import static java.lang.Byte.toUnsignedInt;
import java.util.AbstractList;
import java.util.Arrays;

/**
 * Specialized storage for StorageSlot instances.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 3/17/2015
 */
public final class StorageSlotList extends AbstractList<StorageSlot> {
	private static final byte HOLE = (byte)0xFF, DUP = (byte)0xFE;
	private static final int MAX_TOKENS = toUnsignedInt(DUP);
	//TODO: we never prune unused tokens -- would it ever be worth it?
	/**
	 * A list of Tokens referred to by StorageSlots in this list.
	 */
	private Token[] tokens;
	/**
	 * The currently used size of {@link #tokens}.
	 */
	private int tokensSize;
	/**
	 * For the StorageSlot at index i, {@code tokenIdx[i]} is the index of its
	 * Token in {@link #tokens}, or the special values HOLE and DUP.
	 */
	private byte[] tokenIdx;
	/**
	 * For the StorageSlot at index i, {@code indices[i]} is its index, or
	 * Integer.MIN_VALUE for holes and duplicates (mostly for debugging).
	 */
	private int[] indices;
	/**
	 * The currently used size of {@link #tokenIdx} and {@link #indices}.
	 */
	private int size;
	/**
	 * Looks up a token in {@link #tokens}, or inserts it if not already
	 * present, returning its index.
	 */
	private int insertToken(Token t) {
		//we could keep tokens sorted and do binary search, I guess
		//(though we'd have to rewrite tokenIdx, so maybe not!
		for (int i = 0; i < tokensSize; ++i)
			if (tokens[i].equals(t))
				return i;
		if (tokensSize >= tokens.length) {
			if (tokens.length >= MAX_TOKENS)
				throw new UnsupportedOperationException("too many tokens");
			int newLength = Math.min(2 * tokens.length, MAX_TOKENS);
			tokens = Arrays.copyOf(tokens, newLength);
		}
		tokens[tokensSize] = t;
		return tokensSize++;
	}
	/**
	 * Resizes {@link #tokenIdx} and {@link #indices} if they're full.
	 */
	private void maybeResize() {
		if (size == tokenIdx.length)
			ensureCapacity(tokenIdx.length * 2);
	}

	private StorageSlotList(Token[] tokens, int tokensSize, byte[] tokenIdx, int[] indices, int size) {
		this.tokens = tokens;
		this.tokensSize = tokensSize;
		this.tokenIdx = tokenIdx;
		this.indices = indices;
		this.size = size;
	}

	public StorageSlotList() {
		this(new Token[4], 0, new byte[16], new int[16], 0);
	}

	/**
	 * Returns a new StorageSlotList initialized to contain {@code size} holes.
	 * This exists for {@link Compiler2#joinerRemoval()}.
	 * @param size the desired size of the new list
	 * @return a new StorageSlotList initialized to contain {@code size} holes.
	 */
	public static StorageSlotList ofHoles(int size) {
		byte[] tokenIdx = new byte[size];
		Arrays.fill(tokenIdx, HOLE);
		int[] indices = new int[size];
		Arrays.fill(indices, Integer.MIN_VALUE);
		return new StorageSlotList(new Token[4], 0, tokenIdx, indices, size);
	}

	@Override
	public StorageSlot get(int index) {
		if (tokenIdx[index] == HOLE) return StorageSlot.HOLE;
		if (tokenIdx[index] == DUP) return StorageSlot.DUP;
		return StorageSlot.live(tokens[toUnsignedInt(tokenIdx[index])], indices[index]);
	}
	public boolean isLive(int index) {
		return tokenIdx[index] != HOLE;
	}
	public boolean isDrainable(int index) {
		return tokenIdx[index] != HOLE && tokenIdx[index] != DUP;
	}
	public Token getToken(int index) {
		assert isDrainable(index);
		return tokens[tokenIdx[index]];
	}
	public int getIndex(int index) {
		assert isDrainable(index);
		return indices[index];
	}
	@Override
	public boolean add(StorageSlot e) {
		if (e.isHole()) return addHole();
		if (e.isDuplicate()) return addDuplicate();
		return add(e.token(), e.index());
	}
	public boolean add(Token token, int slotIndex) {
		int ti = insertToken(token);
		maybeResize();
		tokenIdx[size] = (byte)(ti & 0xFF);
		indices[size] = slotIndex;
		++size;
		return true;
	}
	public boolean addHole() {
		maybeResize();
		tokenIdx[size] = HOLE;
		indices[size] = Integer.MIN_VALUE;
		++size;
		return true;
	}
	public boolean addDuplicate() {
		maybeResize();
		tokenIdx[size] = DUP;
		indices[size] = Integer.MIN_VALUE;
		++size;
		return true;
	}
	@Override
	public void add(int index, StorageSlot element) {
		throw new UnsupportedOperationException("shouldn't need this");
	}
	@Override
	public StorageSlot set(int index, StorageSlot element) {
		StorageSlot old = get(index);
		if (element.isHole()) setHole(index);
		else if (element.isDuplicate()) setDuplicate(index);
		else set(index, element.token(), element.index());
		return old;
	}
	public void set(int index, Token token, int slotIndex) {
		int ti = insertToken(token);
		tokenIdx[index] = (byte)(ti & 0xFF);
		indices[index] = slotIndex;
	}
	public void setHole(int index) {
		tokenIdx[index] = HOLE;
		indices[index] = Integer.MIN_VALUE;
	}
	public void setDuplicate(int index) {
		tokenIdx[index] = DUP;
		indices[index] = Integer.MIN_VALUE;
	}
	@Override
	public int size() {
		return size;
	}
	@Override
	public void clear() {
		tokensSize = size = 0;
	}
	public void ensureCapacity(int capacity) {
		if (tokenIdx.length < capacity) {
			tokenIdx = Arrays.copyOf(tokenIdx, capacity);
			indices = Arrays.copyOf(indices, capacity);
		}
	}
	public void trimToSize() {
		//we have to use size 1 so our doubling works (or we can do *2 +1).
		tokens = new Token[1];
		tokenIdx = new byte[1];
		indices = new int[1];
	}
}
