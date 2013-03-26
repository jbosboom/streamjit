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
