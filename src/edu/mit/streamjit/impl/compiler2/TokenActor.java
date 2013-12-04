package edu.mit.streamjit.impl.compiler2;

import static com.google.common.base.Preconditions.*;
import com.google.common.reflect.TypeToken;
import edu.mit.streamjit.api.Identity;
import edu.mit.streamjit.api.StreamElement;
import edu.mit.streamjit.impl.blob.Blob.Token;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.ParameterizedType;

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
		//can't call isInput()/isOutput() here, annoying
		super(id < 0 ? TypeToken.of(void.class) : typeVariable(), id < 0 ? typeVariable() : TypeToken.of(void.class));
		this.token = token;
		this.id = id;
		MethodHandle identity = MethodHandles.identity(int.class);
		if (isOutput())
			inputIndexFunctions().add(identity);
		else
			outputIndexFunctions().add(identity);
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
