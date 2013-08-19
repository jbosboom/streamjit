package edu.mit.streamjit.api;

/**
 * A StreamCompiler compiles a stream graph to a CompiledStream.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/20/2012
 */
public interface StreamCompiler {
	/**
	 *
	 * @param <I>
	 * @param <O>
	 * @param stream
	 * @return
	 * @throws IllegalStreamGraphException if the stream graph is illegal or
	 * uses features not supported by this compiler
	 * @throws StreamCompilationFailedException if compilation fails for some
	 * reason other than a stream graph defect
	 */
	public <I, O> CompiledStream compile(OneToOneElement<I, O> stream, Input<I> input, Output<O> output);
}
