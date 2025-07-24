from unittest.mock import patch

import pytest

from saluki.consume import consume

@patch("saluki.consume.Consumer")
def test_go_forwards_with_no_offset_raises(_):
    with pytest.raises(ValueError):
        consume("broker", "topic", go_forwards=True, offset=None)

# test that tries going forwards that consumes from offset

# test that checks start offset

# test that catches exception