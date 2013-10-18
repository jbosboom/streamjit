package edu.mit.streamjit.impl.distributed.common;

import java.io.Serializable;

import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;

/**
 * {@link Controller} and {@link StreamNode} communicate each other by sending
 * and receiving {@link MessageElement}. Received {@link MessageElement} will be
 * processed by an appropriate {@link MessageVisitor}. </p> To avoid unnecessary
 * communication overhead and keep the communication between the StreamJit nodes
 * optimal, Its advisable to avoid implementing unnecessary functions in any of
 * MessageElements. Instead implement dispatcher to dispatch the enums.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 20, 2013
 */
public interface CTRLRMessageElement extends Serializable {

	public void accept(CTRLRMessageVisitor visitor);

}
