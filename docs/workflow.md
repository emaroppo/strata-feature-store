# The workflow, end to end

Four steps, in order, with the commands that perform them. Everything below has
been run against the two panels currently in the store; the numbers are real.

```
source ──▶ release ──▶ standardise ──▶ feature_group + facts ──▶ compose ──▶ dataset_version
   1          2             3                                        4
```

---

## 0. Before anything: vocabularies

An entity vocabulary is registered once and named by everything that publishes in
it. There is no inference here: a source declares what its ids are, and a typo
would otherwise mint a private vocabulary nothing can join to.

```bash
feature-store namespace iso3166-1-alpha3 --external-ref https://www.iso.org/iso-3166-country-codes.html
feature-store namespace wdi-country --description "World Bank codes; 215 of 266 are ISO3"
```

## 1. Register the source

The publisher's *product*, not a file. Licence, URL, vocabulary and granularity
are declared here once rather than repeated in every config, and standardise
reads them back.

```bash
feature-store source register wdi \
    --namespace wdi-country --granularity annual \
    --licence CC-BY-4.0 --url https://datatopics.worldbank.org/world-development-indicators/
```

## 2. Store the release

One downloaded file, byte-identical to what the publisher served. Nothing is
parsed, unzipped or normalised: this is what makes a mapping mistake recoverable
later, because re-standardising beats re-downloading and hoping the publisher
still serves the same vintage.

```bash
feature-store release add WDI_CSV/WDICSV.csv --source wdi --released-at 2025-06-05
```

`--released-at` is the publisher's vintage, **not** the download date. Conflating
them makes every as-of query before today return nothing, which reads as missing
data rather than as a mistake.

The file is content-addressed, so re-adding identical bytes is a no-op and a
revised file of the same name lands on a different key with both kept.

### Crosswalks are ordinary sources

A source publishing in a non-canonical vocabulary needs one, and standardise
refuses without it.

```bash
uv run --with pycountry python scripts/make_iso3_crosswalk.py \
    --file WDI_CSV/WDICSV.csv --code-column "Country Code" --name-column "Country Name" \
    > wdi_to_iso3.csv

feature-store source register wdi_to_iso3_src --namespace wdi-country \
    --granularity irregular --kind crosswalk
feature-store release add wdi_to_iso3.csv --source wdi_to_iso3_src --released-at 2026-08-26
feature-store crosswalk wdi_to_iso3 <digest>
```

Registration validates one-to-one, so a version that exists is safe to apply.

## 3. Standardise

The config maps columns onto the seven-column schema and does nothing else. It
cannot compute, rescale or impute — wanting an expression language here is the
post-processing repository asking to exist.

```bash
feature-store standardise --spec configs/sources/wdi.json <release-digest>
```

```
wrote 7,491,089 facts, 1,516 features registered
  group     wdi (id 5)
  partition 33
```

What happens, and what it refuses:

- **Entity ids are canonicalised** against the pinned crosswalk. Ids with no
  canonical form go to a sibling group in the native vocabulary — 1,544,314
  facts here, almost all regional aggregates — rather than being dropped.
- **Nulls produce no rows.** A null cell means "not reported", and the group's
  `retraction_policy` says whether the publisher means otherwise.
- **A value that will not cast is an error.** This is what found 25 text columns
  hiding among the seed panel's 3,931 numerics; they became their own group.
- **Event times must fall on the declared boundary**, which catches a wrong
  anchor before it shifts every derived knowledge time.
- **The vocabulary file rides along**, if the config names one, giving each
  feature its unit and definition.

Re-running with the same config is a no-op. Re-running with a *changed* config
writes new parts and supersedes the old, so facts are never counted twice.

## 4. Compose

A spec names a spine — entities crossed with timestamps — and resolves each
feature to what was knowable at that row's cutoff.

```bash
feature-store compose --spec configs/compositions/cross_source.json --preview 10
feature-store compose --spec configs/compositions/cross_source.json     # freeze
feature-store read cross_source@1
```

```
entity spine         polyarchy knowable     gdp_growth  knowable      population
BRA    2016-12-31        0.863 2016-03-12   -3.5457634  2016-03-31    201675532
```

Two publishers, different vocabularies, different cadences, **different
publication schedules** — resolved per feature against its own timeline. 27 rows
in 0.11 seconds.

Freezing pins the spec, the cutoff **and** the ingest watermark. A later read
warns about anything the watermark withholds, and `--latest` shows what has been
learned since.

---

## Afterwards

```bash
feature-store runs show <id> --lineage    # raw bytes ▸ crosswalk ▸ parquet ▸ version
feature-store verify --deep --orphans     # the catalog against the store
feature-store verify --prune-orphans      # remove debris; refuses if anything is mid-write
feature-store runs undo <id>              # roll a run back; refuses if anything depends on it
```

`verify` on the current store: 42 objects, no orphans, no drift, 6.7 seconds for
a full re-hash.
