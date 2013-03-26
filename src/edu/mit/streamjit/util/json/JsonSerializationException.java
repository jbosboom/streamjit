package edu.mit.streamjit.util.json;

import javax.json.JsonValue;

/**
 * Thrown to indicate errors in JSON serialization or deserialization.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/25/2013
 */
public class JsonSerializationException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	/**
	 * A JsonValue involved in the exception, to aid in debugging.  May be null.
	 */
	private final JsonValue value;

	public JsonSerializationException(String message) {
		this(message, null, null);
	}

	public JsonSerializationException(Throwable cause) {
		this(null, cause, null);
	}

	public JsonSerializationException(String message, Throwable cause) {
		this(message, cause, null);
	}

	public JsonSerializationException(String message, JsonValue value) {
		this(message, null, value);
	}

	public JsonSerializationException(String message, Throwable cause, JsonValue value) {
		super(message, cause);
		this.value = value;
	}

	@Override
	public String getMessage() {
		String msg = super.getMessage();
		if (value == null)
			return msg; //if both null, this returns null
		if (msg == null)
			return value.toString(); //know value != null from above
		return msg +": "+value; //know both != null
	}
}
