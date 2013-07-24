package edu.mit.streamjit.impl.distributed.common;

import java.io.Serializable;

import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;

/**
 * {@link Controller} and {@link StreamNode} communicate each other by sending and receiving {@link MessageElement}. Received
 * {@link MessageElement} will be processed by an appropriate {@link MessageVisitor}.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 20, 2013
 */
public interface MessageElement extends Serializable {

	public void accept(MessageVisitor visitor);

}
