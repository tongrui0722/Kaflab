import struct
from app.protocol import codec
from app.protocol import sasl
from app.models import error_name


def test_string_roundtrip():
    for s in ["", "test-topic", "hello kafka", None]:
        assert codec.dec_string(codec.enc_string(s), 0)[0] == s


def test_metadata_v1_controller_and_broker_rack():
    body = struct.pack(">i", 1)
    body += struct.pack(">i", 2) + codec.enc_string("broker-2")
    body += struct.pack(">i", 19092) + codec.enc_string("rack-a")
    body += struct.pack(">i", 2)  # controller id
    body += struct.pack(">i", 0)  # no topic metadata requested
    metadata = codec.decode_metadata_v1(body)
    assert metadata.controller_id == 2
    assert metadata.brokers == [{"id": 2, "host": "broker-2", "port": 19092, "rack": "rack-a"}]


def test_topic_create_error_codes_match_kafka_protocol():
    assert error_name(20) == "NOT_ENOUGH_REPLICAS_AFTER_APPEND"
    assert error_name(36) == "TOPIC_ALREADY_EXISTS"
    assert error_name(41) == "NOT_CONTROLLER"
    assert error_name(42) == "INVALID_REQUEST"
    assert error_name(73) == "TOPIC_DELETION_DISABLED"


def test_delete_topics_v3_response_decodes_throttle_and_error():
    body = struct.pack(">ii", 0, 1) + codec.enc_string("sample") + struct.pack(">h", 73)
    decoded = codec.decode_delete_topics_v1_to_v3(body)
    assert decoded == {"throttleMs": 0, "topics": [{"topic": "sample", "errorCode": 73}],
                       "_end": len(body)}


def test_build_request_frame_len():
    f = codec.build_request(3, 0, 7, "kafka-sim-01", codec.encode_metadata_v0(["t"]))
    (ln,) = struct.unpack_from(">i", f, 0)
    assert ln == len(f) - 4
    api, ver, corr = struct.unpack_from(">hhi", f, 4)
    assert (api, ver, corr) == (3, 0, 7)


def test_alter_reassignments_v0_uses_flexible_encoding():
    body = codec.encode_alter_reassignments_v0("t", {2: [1, 3]})
    frame = codec.build_request(45, 0, 1, "test", body, flexible=True)
    assert frame[-1] == 0  # root tagged fields buffer
    # api header carries flexible header v2's empty tagged fields buffer.
    assert frame[4 + 2 + 2 + 4 + 2 + len("test")] == 0


def test_decode_alter_reassignments_v0_flexible_response():
    body = b"\x00"  # response header v1 tags
    body += struct.pack(">ih", 0, 0)  # throttle, top-level error
    body += b"\x00"  # null compact error message
    body += b"\x02"  # one topic
    body += codec.enc_compact_string("t") + b"\x02"  # one partition
    body += struct.pack(">ih", 2, 0) + b"\x00\x00"  # index, error, null msg, tags
    body += b"\x00\x00"  # topic and root tags
    decoded = codec.decode_alter_reassignments_v0(body)
    assert decoded["errorCode"] == 0
    assert decoded["topics"][0]["topic"] == "t"
    assert decoded["topics"][0]["partitions"][0]["partition"] == 2
    assert decoded["_end"] == len(body)


def test_decode_metadata_v0_single():
    raw = struct.pack(">i", 1) + struct.pack(">i", 101) + codec.enc_string("h") + struct.pack(">i", 9092)
    raw += struct.pack(">i", 1)
    raw += struct.pack(">h", 0) + codec.enc_string("test-topic")
    raw += struct.pack(">i", 1)
    raw += struct.pack(">hii", 0, 2, 103)
    raw += struct.pack(">i", 3) + struct.pack(">iii", 103, 101, 102)
    raw += struct.pack(">i", 3) + struct.pack(">iii", 103, 101, 102)
    m = codec.decode_metadata_v0(raw)
    assert m.brokers[0]["id"] == 101
    assert m.topics[0].partitions[0].leader == 103


def test_record_batch_roundtrip_shape():
    b = codec.encode_record_batch(b"user001", b"hello kafka")
    assert len(b) > 30
    p = codec.encode_produce_v2("test-topic", 2, b"k", b"v")
    assert len(p) > len(b)


def _resp(api, ver, corr, body) -> bytes:
    return struct.pack(">i", corr) + body


def test_sasl_plain_roundtrip_shape():
    init = sasl.plain_initial("", "scramuser", "pw")
    assert init == b"\x00scramuser\x00pw"
    assert sasl.encode_sasl_handshake_v1("SCRAM-SHA-512") == codec.enc_string("SCRAM-SHA-512")


def test_sasl_auth_v1_response_decodes_error_message_before_auth_bytes():
    auth_bytes = b"r=servernonce,s=c2FsdA==,i=4096"
    raw = struct.pack(">h", 0) + codec.enc_string(None) + codec.enc_bytes(auth_bytes)
    raw += struct.pack(">q", 60000)
    decoded = sasl.decode_sasl_auth_v1(raw)
    assert decoded == {"errorCode": 0, "errorMessage": None,
                       "message": auth_bytes, "sessionId": 60000}


def test_sasl_auth_v1_exposes_broker_auth_error_message():
    error = "Authentication failed"
    raw = struct.pack(">h", 58) + codec.enc_string(error) + codec.enc_bytes(b"")
    raw += struct.pack(">q", 0)
    decoded = sasl.decode_sasl_auth_v1(raw)
    assert decoded["errorCode"] == 58
    assert decoded["errorMessage"] == error


def test_scram_client_final_verifies():
    import hashlib
    bare, first, _ = sasl.scram_client_first("scramuser")
    assert first.startswith("n,,n=scramuser,r=")
    first_bare = first[3:]
    server_first = f"r={first_bare[9:] + 'extra'},s=c2FsdA==,i=4096"
    final, sig = sasl.scram_client_final("Scram@123456", first_bare, server_first,
                                         hashlib.sha256)
    assert b"p=" in final
    server_final = ("v=" + __import__("base64").b64encode(sig).decode()).encode()
    assert sasl.verify_server_final(server_final, sig)


def test_fetch_v1_decode_with_record():
    batch = codec.encode_record_batch(b"k", b"hello kafka")
    recs = batch
    body = struct.pack(">i", 0)
    body += struct.pack(">i", 1) + codec.enc_string("test-topic")
    body += struct.pack(">i", 1) + struct.pack(">i", 0) + struct.pack(">h", 0)
    body += struct.pack(">q", 10240) + struct.pack(">q", 10240) + struct.pack(">q", 0)
    body += struct.pack(">i", 0) + codec.enc_bytes(recs)
    dec = codec.decode_fetch_v1(body)
    assert dec["topics"][0]["partitions"][0]["messageCount"] == 1
    assert dec["messages"][0]["value"] == "hello kafka"


def test_join_sync_codec_shapes():
    j = codec.encode_join_group_v1("g", "", ["test-topic"])
    assert len(j) > 20
    s = codec.encode_sync_group_v1("g", 1, "m", [("m", [("test-topic", [0, 1])])])
    assert len(s) > 10
    d = codec.decode_describe_groups_v1(struct.pack(">i", 0))
    assert d == {"groups": []}
    lg = codec.decode_list_groups_v1(struct.pack(">h", 0) + struct.pack(">i", 0))
    assert lg["groups"] == []
