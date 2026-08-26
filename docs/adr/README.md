# Decision records

One file per decision that would otherwise have to be re-derived. Each says what
was decided, what it rules out, and — most importantly — what goes wrong under
the alternative, because that is the part nobody remembers six months later.

A decision belongs here when reversing it would be expensive, when the obvious
alternative is defensible, or when the reasoning is not visible from the code.
Not every choice needs one; a record that merely restates an implementation is
noise.

`docs/architecture.md` §15 lists the decisions still owed a record.
