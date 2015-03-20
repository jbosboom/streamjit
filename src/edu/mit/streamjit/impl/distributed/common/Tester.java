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
 * @since May 20, 2013
 */
package edu.mit.streamjit.impl.distributed.common;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.mit.streamjit.impl.distributed.common.AsyncTCPConnection.AsyncTCPConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionType;
import edu.mit.streamjit.impl.distributed.common.Connection.GenericConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionInfo;

;

public class Tester {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// test1();
		// test2();
		test3();
	}

	/**
	 * Testing one - tests the size of an object.
	 */
	private static void test1() {
		Error er = Error.FILE_NOT_FOUND;
		AppStatus apSts = AppStatus.STOPPED;
		ByteArrayOutputStream byteAos = new ByteArrayOutputStream();

		ObjectOutputStream os = null;
		try {
			os = new ObjectOutputStream(byteAos);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			os.writeObject(apSts);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			os.writeInt(34345);
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println(byteAos.toByteArray().length);
	}

	/**
	 * Tests the equals and hascode.
	 */
	private static void test2() {

		ConnectionInfo asyConInfo1 = new AsyncTCPConnectionInfo(1, 4, 8989);
		ConnectionInfo asyConInfo2 = new AsyncTCPConnectionInfo(1, 4, 8980);
		ConnectionInfo asyConInfo3 = new AsyncTCPConnectionInfo(4, 1, 8989);
		ConnectionInfo asyConInfo4 = new AsyncTCPConnectionInfo(4, 1, 8980);
		ConnectionInfo asyConInfo5 = new AsyncTCPConnectionInfo(1, 4, 8989);

		ConnectionInfo tcpConInfo1 = new TCPConnectionInfo(1, 4, 8989);
		ConnectionInfo tcpConInfo2 = new TCPConnectionInfo(1, 4, 8980);
		ConnectionInfo tcpConInfo3 = new TCPConnectionInfo(4, 1, 8989);
		ConnectionInfo tcpConInfo4 = new TCPConnectionInfo(4, 1, 8980);

		ConnectionInfo conInfo1 = new GenericConnectionInfo(1, 4, true);
		ConnectionInfo conInfo2 = new GenericConnectionInfo(1, 4, false);
		ConnectionInfo conInfo3 = new GenericConnectionInfo(4, 1, true);
		ConnectionInfo conInfo4 = new GenericConnectionInfo(4, 1, false);

		System.out.println("AsyncTCPConnectionInfo - AsyncTCPConnectionInfo");
		System.out.println(asyConInfo1.equals(asyConInfo2));
		System.out.println(asyConInfo1.equals(asyConInfo3));
		System.out.println(asyConInfo1.equals(asyConInfo4));
		System.out.println(asyConInfo2.equals(asyConInfo3));
		System.out.println(asyConInfo2.equals(asyConInfo4));
		System.out.println(asyConInfo3.equals(asyConInfo4));
		System.out.println();

		System.out.println("ConnectionInfo - AsyncTCPConnectionInfo");
		System.out.println(conInfo1.equals(asyConInfo1));
		System.out.println(conInfo1.equals(asyConInfo2));
		System.out.println(conInfo1.equals(asyConInfo3));
		System.out.println(conInfo1.equals(asyConInfo4));
		System.out.println(conInfo2.equals(asyConInfo1));
		System.out.println(conInfo2.equals(asyConInfo2));
		System.out.println(conInfo2.equals(asyConInfo3));
		System.out.println(conInfo2.equals(asyConInfo4));
		System.out.println(conInfo3.equals(asyConInfo1));
		System.out.println(conInfo3.equals(asyConInfo2));
		System.out.println(conInfo3.equals(asyConInfo3));
		System.out.println(conInfo3.equals(asyConInfo4));
		System.out.println(conInfo4.equals(asyConInfo1));
		System.out.println(conInfo4.equals(asyConInfo2));
		System.out.println(conInfo4.equals(asyConInfo3));
		System.out.println(conInfo4.equals(asyConInfo4));
		System.out.println();

		System.out.println("ConnectionInfo - TCPConnectionInfo");
		System.out.println(conInfo1.equals(tcpConInfo1));
		System.out.println(conInfo1.equals(tcpConInfo2));
		System.out.println(conInfo1.equals(tcpConInfo3));
		System.out.println(conInfo1.equals(tcpConInfo4));
		System.out.println(conInfo2.equals(tcpConInfo1));
		System.out.println(conInfo2.equals(tcpConInfo2));
		System.out.println(conInfo2.equals(tcpConInfo3));
		System.out.println(conInfo2.equals(tcpConInfo4));
		System.out.println(conInfo3.equals(tcpConInfo1));
		System.out.println(conInfo3.equals(tcpConInfo2));
		System.out.println(conInfo3.equals(tcpConInfo3));
		System.out.println(conInfo3.equals(tcpConInfo4));
		System.out.println(conInfo4.equals(tcpConInfo1));
		System.out.println(conInfo4.equals(tcpConInfo2));
		System.out.println(conInfo4.equals(tcpConInfo3));
		System.out.println(conInfo4.equals(tcpConInfo4));
		System.out.println();

		Map<ConnectionInfo, Integer> tesMap = new HashMap<>();
		tesMap.put(tcpConInfo1, 1);
		tesMap.put(asyConInfo1, 2);

		System.out.println(tesMap.containsKey(tcpConInfo1));
		System.out.println(tesMap.containsKey(tcpConInfo2));
		System.out.println(tesMap.containsKey(tcpConInfo3));
		System.out.println(tesMap.containsKey(tcpConInfo4));

		System.out.println(tesMap.containsKey(asyConInfo1));
		System.out.println(tesMap.containsKey(asyConInfo2));
		System.out.println(tesMap.containsKey(asyConInfo3));
		System.out.println(tesMap.containsKey(asyConInfo4));
		System.out.println(tesMap.containsKey(asyConInfo5));

		System.out.println(tesMap.containsKey(conInfo1));
		System.out.println(tesMap.containsKey(conInfo2));
		System.out.println(tesMap.containsKey(conInfo3));
		System.out.println(tesMap.containsKey(conInfo4));
	}

	private static void test3() {
		List<ConnectionType> conlist = Arrays.asList(ConnectionType.values());
		for (ConnectionType connectionType : conlist) {
			System.out.println(connectionType);
		}
	}
}
