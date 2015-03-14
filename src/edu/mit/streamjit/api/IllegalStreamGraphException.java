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
package edu.mit.streamjit.api;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;

/**
 * Thrown when a stream graph is malformed.  This indicates a programming error
 * in the code that built the stream graph or in the implementation of one or
 * more StreamElements in the graph.
 *
 * IllegalStreamGraphException implements Serializable, but the StreamElements
 * it refers to usually do not.  When an IllegalStreamGraphException is
 * serialized, any StreamElements that do not implement Serializable are
 * replaced with dummy StreamElement implementations that have the same
 * toString() but without any other data or behavior.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 11/20/2012
 */
public class IllegalStreamGraphException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	/**
	 * A list of one or more stream elements involved in the malformation, to
	 * aid in debuggging.
	 *
	 * Effectively final, but we have to make it non-final for serialization.
	 */
	private transient StreamElement<?, ?>[] elements;

	public IllegalStreamGraphException(Throwable cause) {
		this(null, cause);
	}

	public IllegalStreamGraphException(String message, StreamElement<?, ?>... elements) {
		this(message, null, elements);
	}

	public IllegalStreamGraphException(String message, Throwable cause, StreamElement<?, ?>... elements) {
		super(message, cause);
		this.elements = elements.clone();
	}

	/**
	 * Returns an array of elements involved in the malformation, to aid in
	 * debugging.  May be a 0-length array.
	 *
	 * If this IllegalStreamGraphException was deserialized, the StreamElements
	 * aren't useful for anything but their toString() method.  Calling other
	 * methods (or passing them as parameters to methods) will throw exceptions.
	 * @return an array of elements involved in the malformation
	 */
	public StreamElement<?, ?>[] getElements() {
		return elements.clone();
	}

	@Override
	public String getMessage() {
		String msg = super.getMessage();
		if (elements.length == 0)
			return msg;
		if (msg == null)
			return Arrays.toString(elements);
		return msg + ": " + Arrays.toString(elements);
	}

	//<editor-fold defaultstate="collapsed" desc="Custom serialization">
	private void writeObject(ObjectOutputStream oos) throws IOException {
		oos.defaultWriteObject();
		StreamElement<?, ?>[] serializableElements = new StreamElement<?, ?>[elements.length];
		for (int i = 0; i < elements.length; ++i)
			serializableElements[i] = (elements[i] instanceof Serializable) ? elements[i] : new DummyStreamElement(elements[i].toString());
		oos.writeObject(serializableElements);
	}

	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();
		this.elements = (StreamElement<?, ?>[])ois.readObject();
	}

	private static final class DummyStreamElement implements StreamElement<Void, Void>, Serializable {
		private static final long serialVersionUID = 1L;
		private final String toString;
		private DummyStreamElement(String toString) {
			this.toString = toString;
		}
		@Override
		public void visit(StreamVisitor v) {
			throw new UnsupportedOperationException("Not a real StreamElement!");
		}
		@Override
		public String toString() {
			return toString;
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException {
		StreamElement<?, ?> identity = new Identity<>();
		IllegalStreamGraphException isge = new IllegalStreamGraphException("message", identity);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(isge);
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		ObjectInputStream ois = new ObjectInputStream(bais);
		IllegalStreamGraphException ex = (IllegalStreamGraphException)ois.readObject();

		ex.printStackTrace(System.out);
		System.out.println(ex.getElements()[0].toString());
		System.out.println(ex.getElements()[0].getClass());
	}
	//</editor-fold>
}
