/*
 * Copyright (c) 2013-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package edu.mit.streamjit.util.json;

import javax.json.JsonValue;

/**
 * Thrown to indicate errors in JSON serialization or deserialization.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 3/25/2013
 */
public final class JsonSerializationException extends RuntimeException {
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
