package edu.mit.streamjit.api;

/**
 * Prints the stream elements. {@link StreamPrinter} could be used as a sink to prints the final stream output. TODO: Does
 * IntermediateStreamPrinter that can be plugged in between two {@link OneToOneElement}s to prints the stream elements passing through
 * channel needed?
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Apr 29, 2013
 */
public class StreamPrinter<T> extends Filter<T, Void> {

	public StreamPrinter() {
		super(1, 0);
	}

	@Override
	public void work() {
		System.out.println(pop().toString());
	}
}