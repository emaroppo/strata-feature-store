"""Emit a crosswalk from the seed panel's entity vocabulary to ISO 3166-1 alpha-3.

Run against a release to produce a CSV that can be ingested as a crosswalk
source. Committed so the mapping is reproducible rather than a file someone once
made: a crosswalk decides which rows survive a join, and one that nobody can
regenerate is one nobody can check.

**Identity mappings are written explicitly.** 175 of the 202 codes already are
ISO3, and it would be tempting to treat "looks like ISO3" as "is ISO3" at query
time. That inference is what the crosswalk exists to replace — the whole failure
mode is codes that look right and mean something else.

**The 27 that do not map get no row, deliberately.** Three are withdrawn ISO3
(DDR, VDR, YMD); the rest are pre-unification German and Italian states, plus
four entities with data through 2024 — Kosovo, Somaliland, Palestine/Gaza and
Zanzibar — that have no ISO3 at all. They are not errors and they are not
dropped: standardisation routes unmapped entities to a sibling group in the
native vocabulary, where they stay available and stay unjoinable without a
crosswalk of their own. Inventing codes for them here would be minting a private
vocabulary that then needs maintaining, and succession belongs to a knowledge
layer that does not exist yet.

Usage:
    uv run --with pycountry python scripts/make_vdem_iso3_crosswalk.py \\
        V-Dem-CD-v15.csv > vdem_to_iso3.csv
"""

from __future__ import annotations

import csv
import sys

#: Wide enough to cover the panel, which starts in 1789.
VALID_FROM = "1789-01-01"
VALID_TO = "9999-12-31"


def main(path: str) -> int:
    import pycountry

    iso3 = {country.alpha_3 for country in pycountry.countries}

    codes: dict[str, str] = {}
    with open(path, newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader)
        code_at = header.index("country_text_id")
        name_at = header.index("country_name")
        for row in reader:
            codes.setdefault(row[code_at], row[name_at])

    writer = csv.writer(sys.stdout)
    writer.writerow(["source_id", "target_id", "valid_from", "valid_to", "confidence"])
    mapped = 0
    for code in sorted(codes):
        if code in iso3:
            writer.writerow([code, code, VALID_FROM, VALID_TO, "1.0"])
            mapped += 1

    unmapped = sorted(c for c in codes if c not in iso3)
    print(
        f"{len(codes)} codes: {mapped} mapped, {len(unmapped)} with no ISO3 "
        f"({', '.join(unmapped)})",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        raise SystemExit(2)
    raise SystemExit(main(sys.argv[1]))
