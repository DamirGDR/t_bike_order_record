from zone_exit import decode_polyline, point_in_polygon, still_outside_service, zone_exit_action

_GOOGLE = "_p~iF~ps|U_ulLnnqC_mqNvxq`@"


def test_zone_exit_waits_until_confirm_window() -> None:
    assert zone_exit_action(14, True) == "wait"
    assert zone_exit_action(None, True) == "wait"


def test_zone_exit_sends_only_when_still_outside() -> None:
    assert zone_exit_action(15, True) == "send"
    assert zone_exit_action(15, None) == "send"
    assert zone_exit_action(15, False) == "skip"


def test_decode_google_example() -> None:
    points = decode_polyline(_GOOGLE)
    assert points[0] == (38.5, -120.2)
    assert points[1] == (40.7, -120.95)
    assert points[2] == (43.252, -126.453)


def test_point_in_polygon_square() -> None:
    square = [(0.0, 0.0), (0.0, 3.0), (3.0, 3.0), (3.0, 0.0)]
    assert point_in_polygon(1, 1, square) is True
    assert point_in_polygon(4, 4, square) is False


def test_still_outside_service_missing_fence_is_unknown() -> None:
    assert still_outside_service(1, 1, "") is None
    assert still_outside_service(None, 1, "abc") is None
    assert still_outside_service(0, 0, "abc") is None
