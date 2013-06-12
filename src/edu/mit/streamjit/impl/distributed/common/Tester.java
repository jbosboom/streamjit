/**
 * @author Sumanan sumanan@mit.edu
 * @since May 20, 2013
 */
package edu.mit.streamjit.impl.distributed.runtime.common;

import edu.mit.streamjit.impl.distributed.runtime.api.AppStatus;
import edu.mit.streamjit.impl.distributed.runtime.api.Command;
import edu.mit.streamjit.impl.distributed.runtime.api.MessageElement;
import edu.mit.streamjit.impl.distributed.runtime.api.MessageVisitor;

public class Tester {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		MessageElement sts = AppStatus.RUNNING;
		MessageElement command = Command.START;
	
	}
}
