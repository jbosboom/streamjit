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
 * Contains interfaces for a simple JSON object binding system.
 *
 * To add support for a type:
 * <ol>
 * <li>Implement a Jsonifier that can convert instances of the type to and
 * from JSON values.</li>
 * <li>Implement a JsonifierFactory that responds to Class objects representing
 * the type by returning the Jsonifier from the previous step.</li>
 * <li>Register the JsonifierFactory in
 * src/META-INF/services/edu.mit.streamjit.util.json.JsonifierFactory.</li>
 * </ol>
 *
 * Note that one JsonifierFactory can return Jsonifier instances for multiple
 * types and Jsonifier can implement conversion for multiple types.  Also note
 * that Jsonifier and JsonifierFactory can be the same class.
 *
 * This system does not serialize reference graphs; if an object is encountered
 * multiple times, it will be serialized multiple times and deserialized into
 * multiple separate objects.  If you need graph support, consider using Java
 * serialization into a byte[], then JSON-ifying a base64-encoded string.
 */
package edu.mit.streamjit.util.json;
