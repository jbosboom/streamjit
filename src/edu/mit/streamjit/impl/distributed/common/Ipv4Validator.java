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
/**
 * @author Sumanan sumanan@mit.edu
 * @since May 15, 2013
 */
package edu.mit.streamjit.impl.distributed.common;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Ipv4Validator {

	private Pattern pattern;
	private Matcher matcher;

	private static Ipv4Validator instance;

	private static final String IPADDRESS_PATTERN = "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\."
			+ "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\."
			+ "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\."
			+ "([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";

	public static Ipv4Validator getInstance() {
		if (instance == null)
			instance = new Ipv4Validator();
		return Ipv4Validator.instance;
	}

	private Ipv4Validator() {
		pattern = Pattern.compile(IPADDRESS_PATTERN);
	}

	public boolean isValid(String ipv4Address) {
		matcher = pattern.matcher(ipv4Address);
		return matcher.matches();
	}

	public boolean isValid(int portNo) {
		if (portNo > 0 && portNo < 65535)
			return true;
		else
			return false;
	}
}
