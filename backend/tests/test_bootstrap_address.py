from app.network.tcp import parse_hostport


def test_comma_separated_bootstrap_uses_first_nonempty_seed():
    assert parse_hostport(" 192.168.127.130:30092 , 192.168.127.129:30092 ") == (
        "192.168.127.130", 30092)
    assert parse_hostport(" , broker2:9093 ") == ("broker2", 9093)


def test_single_bootstrap_address_remains_supported():
    assert parse_hostport("broker1:9092") == ("broker1", 9092)
