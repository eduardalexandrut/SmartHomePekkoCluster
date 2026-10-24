package org.example;

/**
 * Marker interface for every message that can travel between cluster nodes.
 *
 * application.conf binds this interface to Pekko's Jackson CBOR serializer, so
 * any message implementing it is serialized automatically when it is sent to an
 * actor on another node.
 */
public interface CborSerializable {
}
