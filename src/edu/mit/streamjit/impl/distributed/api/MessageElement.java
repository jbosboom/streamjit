/**
 * @author Sumanan sumanan@mit.edu
 * @since May 20, 2013
 */
package edu.mit.streamjit.impl.distributed.runtime.api;

import java.io.Serializable;

public interface MessageElement extends Serializable {

	public void accept(MessageVisitor visitor);

}
