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

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionInfo;

public abstract class MiscCtrlElements implements CTRLRMessageElement {

	private static final long serialVersionUID = 1L;

	public abstract void process(MiscCtrlElementProcessor miscProcessor);

	@Override
	public void accept(CTRLRMessageVisitor visitor) {
		visitor.visit(this);
	}

	public static final class NewConInfo extends MiscCtrlElements {
		private static final long serialVersionUID = 1L;

		public final TCPConnectionInfo conInfo;
		public final Token token;

		public NewConInfo(TCPConnectionInfo conInfo, Token token) {
			this.conInfo = conInfo;
			this.token = token;
		}

		@Override
		public void process(MiscCtrlElementProcessor miscProcessor) {
			miscProcessor.process(this);
		}
	}

	public interface MiscCtrlElementProcessor {

		public void process(NewConInfo newConInfo);
	}
}