"""Emit a crosswalk from a source's entity vocabulary to ISO 3166-1 alpha-3.

Run against a release to produce a CSV that can be ingested as a crosswalk
source. Committed so the mapping is reproducible rather than a file someone once
made: a crosswalk decides which rows survive a join, and one that nobody can
regenerate is one nobody can check.

**Identity mappings are written explicitly.** Most codes in a country panel
already are ISO3, and it would be tempting to treat "looks like ISO3" as "is
ISO3" at query time. That inference is what the crosswalk exists to replace —
the whole failure mode is codes that look right and mean something else.

**Codes with no ISO3 get no row, deliberately.** They are neither errors nor
dropped: standardisation routes unmapped entities to a sibling group in the
source's native vocabulary, where they stay available and stay unjoinable
without a crosswalk of their own. That is the right home for a regional
aggregate, which is a real row describing something that is not a country.
Inventing codes for them here would mint a private vocabulary that then needs
maintaining, and succession belongs to a knowledge layer that does not exist yet.

Usage:
    uv run --with pycountry python scripts/make_iso3_crosswalk.py \\
        --file WDI_CSV/WDICSV.csv --code-column "Country Code" \\
        --name-column "Country Name" > wdi_to_iso3.csv
"""

from __future__ import annotations

import argparse
import csv
import sys

#: Wide enough to cover any panel here; the earliest starts in 1789.
VALID_FROM = "1789-01-01"
VALID_TO = "9999-12-31"


def main() -> int:
    import pycountry

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", required=True)
    parser.add_argument("--code-column", required=True)
    parser.add_argument("--name-column", required=True)
    args = parser.parse_args()

    iso3 = {country.alpha_3 for country in pycountry.countries}

    codes: dict[str, str] = {}
    with open(args.file, newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader)
        try:
            code_at = header.index(args.code_column)
            name_at = header.index(args.name_column)
        except ValueError:
            print(f"columns not found; the file has {header[:8]}...", file=sys.stderr)
            return 2
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
        f"({', '.join(unmapped[:12])}{' ...' if len(unmapped) > 12 else ''})",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
