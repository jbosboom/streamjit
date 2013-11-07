package edu.mit.streamjit.impl.compiler2;

import static com.google.common.base.Preconditions.*;
import edu.mit.streamjit.impl.blob.Blob.Token;

/**
 * An Actor encapsulating a Token, either input xor output.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
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
		this.token = token;
		this.id = id;
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
	public Class<?> inputType() {
		//TODO: throw if isOutput()?
		return Object.class;
	}

	@Override
	public Class<?> outputType() {
		//TODO: throw if isInput()?
		return Object.class;
	}
}
