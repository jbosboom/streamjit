/**
 * Contains interfaces for a simple JSON object binding system.  Classes wishing
 * to participate in the binding should register corresponding Jsonifiers and
 * JsonifierFactories using the ServiceLoader mechanism.
 *
 * This system does not serialize reference graphs; if an object is encountered
 * multiple times, it will be serialized multiple times and deserialized into
 * multiple separate objects.  If you need graph support, consider using Java
 * serialization into a byte[], then JSON-ifying a base64-encoded string.
 */
package edu.mit.streamjit.util.json;
