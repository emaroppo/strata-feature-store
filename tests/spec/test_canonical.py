"""Canonical serialisation: the properties that make a hash mean something.

Each test here corresponds to a way a hash could change without a meaning
changing, or stay the same while a meaning changed. Both directions corrupt
lineage, and neither raises anything at the time.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone

import pytest

from strata.feature_store.spec.canonical import (
    canonical_bytes,
    canonical_json,
    content_hash,
    hash_file,
    short_hash,
)


class TestWhatMustNotChangeTheHash:
    """Reformatting is not a new config."""

    def test_key_order(self):
        assert content_hash({"a": 1, "b": 2}) == content_hash({"b": 2, "a": 1})

    def test_nested_key_order(self):
        left = {"outer": {"z": 1, "a": {"n": 2, "m": 3}}}
        right = {"outer": {"a": {"m": 3, "n": 2}, "z": 1}}
        assert content_hash(left) == content_hash(right)

    def test_whitespace_in_the_source_text(self):
        payload = {"a": [1, 2], "b": "x"}
        indented = json.loads(json.dumps(payload, indent=4))
        assert content_hash(payload) == content_hash(indented)

    def test_an_explicit_default_matches_an_omitted_one_once_materialised(self):
        """Callers materialise defaults before hashing; then the two agree."""
        written = {"anchor": "end", "period": "annual"}
        omitted_then_filled = {"period": "annual"} | {"anchor": "end"}
        assert content_hash(written) == content_hash(omitted_then_filled)


class TestWhatMustChangeTheHash:
    """A different meaning is a different config."""

    def test_a_changed_value(self):
        assert content_hash({"lag": "P18M"}) != content_hash({"lag": "P24M"})

    def test_an_added_field(self):
        assert content_hash({"a": 1}) != content_hash({"a": 1, "b": 2})

    def test_a_schema_version_bump(self):
        """The text can be identical while a field's meaning has moved."""
        assert content_hash({"schema_version": 1, "a": 1}) != content_hash(
            {"schema_version": 2, "a": 1}
        )

    def test_list_order(self):
        """Order is meaningful in a feature list; it decides column order."""
        assert content_hash({"f": ["a", "b"]}) != content_hash({"f": ["b", "a"]})

    def test_a_string_is_not_its_number(self):
        assert content_hash({"v": 1}) != content_hash({"v": "1"})


class TestRefusals:
    """Values with no single reproducible rendering are refused, not guessed."""

    def test_nan(self):
        with pytest.raises(ValueError):
            canonical_json({"x": float("nan")})

    def test_infinity(self):
        with pytest.raises(ValueError):
            canonical_json({"x": float("inf")})

    def test_sets_have_no_order(self):
        with pytest.raises(TypeError, match="no order"):
            canonical_json({"x": {1, 2}})

    def test_aware_datetimes(self):
        """Naive UTC throughout; an offset means something skipped the edge."""
        with pytest.raises(ValueError, match="timezone"):
            canonical_json({"t": datetime(2024, 1, 1, tzinfo=timezone.utc)})

    def test_an_unknown_type(self):
        with pytest.raises(TypeError, match="no canonical form"):
            canonical_json({"x": object()})


class TestRendering:
    def test_dates_render_as_iso(self):
        assert canonical_json({"d": date(2024, 4, 15)}) == '{"d":"2024-04-15"}'

    def test_datetimes_keep_microseconds(self):
        rendered = canonical_json({"t": datetime(2024, 4, 15, 9, 30, 0, 123456)})
        assert rendered == '{"t":"2024-04-15T09:30:00.123456"}'

    def test_no_incidental_whitespace(self):
        assert canonical_json({"a": 1, "b": [1, 2]}) == '{"a":1,"b":[1,2]}'

    def test_bytes_are_utf8_of_the_text(self):
        payload = {"name": "Åland"}
        assert canonical_bytes(payload) == canonical_json(payload).encode("utf-8")


class TestShortHash:
    def test_is_a_prefix_of_the_full_hash(self):
        payload = {"a": 1}
        assert content_hash(payload).startswith(short_hash(payload))

    def test_default_length(self):
        assert len(short_hash({"a": 1})) == 16

    @pytest.mark.parametrize("length", [0, 4, 7, 65])
    def test_refuses_lengths_that_invite_collisions(self, length):
        with pytest.raises(ValueError):
            short_hash({"a": 1}, length)


class TestHashFile:
    """The one hash over raw bytes, because a release *is* its bytes."""

    def test_hashes_the_bytes_and_counts_them(self, tmp_path):
        content = b"a,b\n1,2\n"
        path = tmp_path / "release.csv"
        path.write_bytes(content)
        digest, size = hash_file(str(path))
        assert size == len(content)
        assert digest == hashlib.sha256(content).hexdigest()

    def test_is_stable_across_chunk_sizes(self, tmp_path):
        path = tmp_path / "release.bin"
        path.write_bytes(bytes(range(256)) * 1000)
        assert hash_file(str(path), chunk_size=7)[0] == hash_file(str(path))[0]

    def test_empty_file(self, tmp_path):
        path = tmp_path / "empty"
        path.write_bytes(b"")
        digest, size = hash_file(str(path))
        assert size == 0
        assert digest == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
