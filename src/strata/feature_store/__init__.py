"""Point-in-time correct storage, standardisation and retrieval for revised panel data.

``spec`` holds pure types and functions; ``store`` holds everything that does I/O.
The split lets a sister repository read the composed-output contract without a
database driver or an S3 client, and ``tests/spec/test_purity.py`` enforces it.
"""
