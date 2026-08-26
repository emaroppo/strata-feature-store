# 8. Frozen versions are immutable, enforced by the database

**Status:** accepted

## The decision

`dataset_version` rows cannot be updated. A `BEFORE UPDATE` trigger raises,
naming the row and saying why. Corrections make a new version; that is what
`version_num` is for.

## Why a trigger rather than discipline

Because the tempting fix, while correcting a mistake, is exactly a one-row
`UPDATE`.

A frozen version is the identity of a result. Editing one changes what an
existing pin resolves to **without changing its hash**, which is the quietest way
a reproducible answer stops being reproducible: the spec is unchanged, the hash
is unchanged, and the data is different. Nobody reviewing the version list would
see anything wrong.

Every other invariant here is enforced by structure — lineage by the type
signature, the file list by the catalog rather than a prefix listing. This one
had no structural home, so it got a trigger. A convention that is one `psql`
session away from being broken is not an invariant.

## Consequences

- `run undo` refuses to unmake a run that froze a version, because undoing it
  would leave every pin unresolvable. It names what depends on the outputs
  rather than offering a force flag.
- A fidelity upgrade — a better knowledge time, a corrected crosswalk — produces
  a new version with lineage to the old, and old pins keep resolving to old data.
- Correcting a modelled lag is a metadata update on `feature_group`, not on any
  frozen version, which is why that remains possible.
