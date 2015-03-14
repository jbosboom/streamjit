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
package edu.mit.streamjit.impl.distributed.common;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;

/**
 * {@link Controller} and {@link StreamNode} shall communicate all kind of
 * draining information by exchanging DrainElement. </p> All fields of
 * DrainElement are public and final because the purpose of this class is
 * nothing but exchange data between controller and stream node..
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Jul 29, 2013
 */
public abstract class SNDrainElement implements SNMessageElement {
	private static final long serialVersionUID = 1L;

	public abstract void process(SNDrainProcessor dp);

	@Override
	public void accept(SNMessageVisitor visitor) {
		visitor.visit(this);
	}

	/**
	 * {@link StreamNode}s shall send this object to inform {@link Controller}
	 * that draining of a particular blob is done.
	 */
	public static final class Drained extends SNDrainElement {
		private static final long serialVersionUID = 1L;

		/**
		 * Identifies the blob. Since {@link Blob}s do not have an unique
		 * identifier them self, the minimum input token of that blob is used as
		 * identifier.
		 */
		public final Token blobID;

		public Drained(Token blobID) {
			this.blobID = blobID;
		}

		@Override
		public void process(SNDrainProcessor dp) {
			dp.process(this);
		}
	}

	/**
	 * Contains map of {@link DrainData} of drained {@link Blob}s.
	 * {@link StreamNode}s shall send this back to {@link Controller} to submit
	 * the drain data of the blobs after the draining. See {@link DrainData} for
	 * more information.
	 */
	public static final class DrainedData extends SNDrainElement {
		private static final long serialVersionUID = 1L;

		public final Token blobID;
		public final DrainData drainData;
		public final ImmutableMap<Token, ImmutableList<Object>> inputData;
		public final ImmutableMap<Token, ImmutableList<Object>> outputData;

		public DrainedData(Token blobID, DrainData drainData,
				ImmutableMap<Token, ImmutableList<Object>> inputData,
				ImmutableMap<Token, ImmutableList<Object>> outputData) {
			this.blobID = blobID;
			this.drainData = drainData;
			this.inputData = inputData;
			this.outputData = outputData;
		}

		@Override
		public void process(SNDrainProcessor dp) {
			dp.process(this);
		}
	}

	/**
	 * </p> As sub types of the {@link DrainElement} classes, not enums,
	 * overloaded methods in DrainProcessor is enough. Jvm will automatically
	 * dispatch the sub type of DrainElement to correct matching process() here.
	 * We do not need explicit processXXX() functions as it is done for all
	 * enums such as {@link Error}, {@link AppStatus} and {@link Request}.
	 */
	public interface SNDrainProcessor {

		public void process(Drained drained);

		public void process(DrainedData drainedData);
	}
}
