package edu.mit.streamjit.impl.distributed.common;

import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;

/**
 * This class carries the Json string of a {@link Configuration} object.
 * {@link Controller} sends the json string to {@link StreamNode} with all
 * information of a stream application.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
public class JsonString implements MessageElement {

	private static final long serialVersionUID = -5900812807902330853L;

	private String jsonString;

	public JsonString(String jsonString) {
		this.jsonString = jsonString;
	}

	@Override
	public void accept(MessageVisitor visitor) {
		visitor.visit(this);
	}

	public void process(JsonStringProcessor jp) {
		jp.process(jsonString);
	}

	/**
	 * Processes json string of a {@link Configuration} that is sent by
	 * {@link Controller}.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since May 27, 2013
	 */
	public interface JsonStringProcessor {

		public void process(String json);

	}
}
