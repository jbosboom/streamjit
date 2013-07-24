/**
 * @author Sumanan sumanan@mit.edu
 * @since May 29, 2013
 */
package edu.mit.streamjit.impl.distributed.common;

public interface BoundaryOutputChannel<E> extends BoundaryChannel<E> {

	void sendData();
}
