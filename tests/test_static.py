"""Encoding integrity of static assets.

The dashboard moved from an inline Python string to a static file in
294b2d2, which took it out of Python-source linting; that same commit
introduced double-encoded UTF-8 (mojibake) in six spots — middots
rendered as 'Â·' and em dashes as invisible-control junk in the banner
strings. These tests pin the failure mode: a UTF-8 file misread as
Latin-1 and re-encoded leaves C1 control characters (U+0080–U+009F)
and Â/â artifact pairs that valid text never contains.
"""

from pathlib import Path

import pytest

_STATIC_DIR = Path(__file__).parent.parent / "src" / "sluice" / "static"
_TEXT_ASSETS = sorted(
    p for p in _STATIC_DIR.rglob("*") if p.suffix in {".html", ".css", ".js", ".txt"}
)


@pytest.mark.parametrize("asset", _TEXT_ASSETS, ids=lambda p: p.name)
def test_static_asset_is_valid_utf8_without_mojibake(asset: Path) -> None:
    text = asset.read_bytes().decode("utf-8")  # strict: invalid UTF-8 raises

    c1_controls = [
        (i, hex(ord(c))) for i, c in enumerate(text) if "\x80" <= c <= "\x9f"
    ]
    assert not c1_controls, f"C1 control chars (double-encode residue): {c1_controls}"

    # 'Â' or 'â' followed by another non-ASCII char is the signature of
    # UTF-8 → Latin-1 → UTF-8 round-tripped punctuation (·, —, etc.).
    artifacts = [
        (i, text[i : i + 2])
        for i, c in enumerate(text[:-1])
        if c in "\xc2\xe2" and ord(text[i + 1]) > 127
    ]
    assert not artifacts, f"double-encoded UTF-8 artifacts: {artifacts}"


def test_static_assets_exist() -> None:
    assert any(p.name == "dashboard.html" for p in _TEXT_ASSETS)
