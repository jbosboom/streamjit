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

import static com.google.common.base.Preconditions.*;
import com.google.common.reflect.TypeToken;
import edu.mit.streamjit.api.Identity;
import edu.mit.streamjit.api.StreamElement;
import edu.mit.streamjit.impl.blob.Blob.Token;
import java.lang.reflect.ParameterizedType;

/**
 * An Actor encapsulating a Token, either input xor output.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 10/24/2013
 */
public final class TokenActor extends Actor {
	private final Token token;
	/**
	 * This Actor's identifier.  Input tokens get negative identifiers; output
	 * tokens get massively positive ones.
	 */
	private final int id;
	public TokenActor(Token token, int id) {
		//can't call isInput()/isOutput() here, annoying
		super(id < 0 ? TypeToken.of(void.class) : typeVariable(), id < 0 ? typeVariable() : TypeToken.of(void.class));
		this.token = token;
		this.id = id;
		if (isOutput())
			inputIndexFunctions().add(IndexFunction.identity());
		else
			outputIndexFunctions().add(IndexFunction.identity());
	}
	private static TypeToken<?> typeVariable() {
		//TODO: I want a TypeToken<T>, but Guava goes out of its way to make
		//that hard; is there something else I should do instead?
		ParameterizedType t = (ParameterizedType)TypeToken.of(Identity.class).getSupertype(StreamElement.class).getType();
		return TypeToken.of(t.getActualTypeArguments()[0]);
	}

	@Override
	public int id() {
		return id;
	}

	public Token token() {
		return token;
	}

	public boolean isInput() {
		return id < 0;
	}

	public boolean isOutput() {
		return !isInput();
	}

	@Override
	public int peek(int input) {
		checkState(isOutput());
		checkElementIndex(input, inputs().size());
		return 0;
	}

	@Override
	public int pop(int input) {
		checkState(isOutput());
		checkElementIndex(input, inputs().size());
		return 1;
	}

	@Override
	public int push(int output) {
		checkState(isInput());
		checkElementIndex(output, outputs().size());
		return 1;
	}

	@Override
	public String toString() {
		return String.format("%s@%d[%s]", getClass().getSimpleName(), id(), token);
	}
}
