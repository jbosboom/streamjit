/**
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
package edu.mit.streamjit.impl.distributed.common;

import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;

public class JsonString implements MessageElement {

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
