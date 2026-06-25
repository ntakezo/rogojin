# Agents

## Conventions

Follow conventions in CONVENTIONS.md

## Comments

In addition to the CONVENTIONS.md comments section, you get **0-2 lines max** to comment.

## Export Policy

Litmus test for exporting vs importing; Would you fix a bug in this exported symbol for a stranger, and never rename it without a major version bump? If no, lowercase it or move it to /internal.
Per-symbol checks, each a reason to keep it unexported (lowercase) or in /internal:

- If a type is exported but every useful field on it is unexported, you've shipped an opaque box nobody can construct or inspect — keep the type internal or finish the API.
- If a struct field is only set by your own code, lowercase it; exporting it lets callers put the struct into states your invariants don't cover.
- If a function takes or returns an unexported type, exporting the function is a lie — callers can call it but can't name what it gives back.
- If a var is exported, assume someone will mutate it from another package; if that would corrupt state, make it unexported with an accessor.
- If you exported a constant just for tests, use an export_test.go file instead so it stays out of the public API.
- If an interface is exported but you expect to add methods later, you've frozen it — keep it internal until the method set is stable, since adding a method breaks every implementer.
- If a symbol only exists to be shared between two of your own packages, that's exactly what /internal is for — exported-but-internal, not exported-to-the-world.
