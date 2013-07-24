/**
 * @author Sumanan sumanan@mit.edu
 * @since May 20, 2013
 */
package edu.mit.streamjit.impl.distributed.common;

public interface ErrorProcessor {

	public void processFILE_NOT_FOUND();

	public void processWORKER_NOT_FOUND();

}
