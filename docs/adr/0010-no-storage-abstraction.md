# 10. One storage code path, and a bucket of its own

**Status:** accepted

## The decision

There is no `StorageBackend` interface. Everything goes over the S3 API —
`boto3` for objects, DuckDB `httpfs` for parquet — against one configurable
endpoint. The store owns its own bucket rather than a prefix in a shared one.

## Why no abstraction

An abstraction here would mean the thing that runs in tests is not the thing that
runs in production. That is the gap that makes S3 mocks worthless: they reproduce
the API and not the behaviour.

The behaviour is where the problems live, and all of them were found by running
against a real S3-compatible store rather than a fake:

- path-style addressing, because virtual-host style needs DNS a private instance
  does not have
- a region that must be set even when it means nothing, or the signature fails in
  a way that reads as bad credentials
- an endpoint DuckDB wants without a scheme and boto3 wants with one
- flexible response checksums, which newer botocore validates in a way
  S3-compatible stores do not reproduce over multipart transfers — so it passes
  every small test and fails on the first real file

Each of those reports something other than the setting that caused it. A fake
would have reported none of them.

## Why a separate bucket

Access keys are granted per bucket, so a separate bucket is what keeps this
store's credential away from a neighbour's data and vice versa. Lifecycles
differ, quotas differ, and teardown is one line.

The test suite gets its own bucket for the same reason and a sharper one: it
creates and deletes objects freely. Sharing with real data meant
`verify --orphans` could never say anything useful, because the real answer was
buried under 2,504 test objects.

## Consequences

- The same code runs against Garage, S3, R2 or Ceph by changing `endpoint_url`.
- Every one of those settings lives in one place with a docstring naming the
  failure it prevents, because rediscovering them costs an afternoon each.
