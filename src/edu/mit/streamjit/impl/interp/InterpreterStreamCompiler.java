package edu.mit.streamjit.impl.interp;

import edu.mit.streamjit.impl.common.BlobHostStreamCompiler;

/**
 * A stream compiler that interprets the stream graph.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/29/2013
 */
public final class InterpreterStreamCompiler extends BlobHostStreamCompiler {
	public InterpreterStreamCompiler() {
		super(new Interpreter.InterpreterBlobFactory());
	}
	@Override
	public String toString() {
		return "InterpreterStreamCompiler";
	}
}
