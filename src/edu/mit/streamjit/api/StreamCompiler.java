package edu.mit.streamjit.api;

/**
 * A StreamCompiler compiles a stream graph to a CompiledStream.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/20/2012
 */
public interface StreamCompiler {
	public <I, O> CompiledStream<I, O> compile(OneToOneElement<I, O> stream);
}
