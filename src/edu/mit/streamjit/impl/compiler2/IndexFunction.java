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

import edu.mit.streamjit.util.bytecode.methodhandles.LookupUtils;
import java.lang.invoke.MethodHandle;
import java.util.Objects;
import java.util.function.IntUnaryOperator;

/**
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 3/15/2015
 */
public interface IndexFunction extends IntUnaryOperator {
	public default void applyBulk(int[] bulk) {
		for (int i = 0; i < bulk.length; ++i)
			bulk[i] = applyAsInt(bulk[i]);
	}

	@Override
	public default IndexFunction compose(IntUnaryOperator before) {
		return before instanceof IndexFunction ? new CompoundIndexFunction((IndexFunction)before, this)
				: new CompoundIndexFunction(before::applyAsInt, this);
    }

	@Override
	public default IndexFunction andThen(IntUnaryOperator after) {
		return after instanceof IndexFunction ? new CompoundIndexFunction(this, (IndexFunction)after)
				: new CompoundIndexFunction(this, after::applyAsInt);
    }

	/* private */ static final MethodHandle APPLY_AS_INT = LookupUtils.findVirtual(IntUnaryOperator.class, "applyAsInt");
	public default MethodHandle asHandle() {
		return APPLY_AS_INT.bindTo(this);
	}

	public static IndexFunction identity() {
		return IdentityIndexFunction.INSTANCE;
	}
}

final class IdentityIndexFunction implements IndexFunction {
	static final IndexFunction INSTANCE = new IdentityIndexFunction();
	private IdentityIndexFunction() {}
	@Override
	public int applyAsInt(int operand) {
		return operand;
	}
	@Override
	public void applyBulk(int[] bulk) {
		//do nothing
	}
}

final class CompoundIndexFunction implements IndexFunction {
	private final IndexFunction before, after;
	CompoundIndexFunction(IndexFunction before, IndexFunction after) {
		this.before = Objects.requireNonNull(before);
		this.after = Objects.requireNonNull(after);
	}
	@Override
	public int applyAsInt(int operand) {
		return after.applyAsInt(before.applyAsInt(operand));
	}
	@Override
	public void applyBulk(int[] bulk) {
		before.applyBulk(bulk);
		after.applyBulk(bulk);
	}
}
