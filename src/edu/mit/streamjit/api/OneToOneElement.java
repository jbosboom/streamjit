package edu.mit.streamjit.api;

import edu.mit.streamjit.api.StreamElement;

/**
 * A OneToOneElement is a StreamElement with exactly one input and one output
 * channel.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/22/2012
 */
public interface OneToOneElement<I, O> extends StreamElement<I, O> {
}