/**
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
package edu.mit.streamjit.impl.distributed.runtime.api;

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
}
