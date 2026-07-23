# Conventions

## Comments

Document the “what” and “why,” not just the “how.” Good function documentation explains what a function does, what inputs it expects, what it returns, and any important caveats—not a line-by-line translation of the code.
Use Go's doc conventions: https://go.dev/doc/comment

## Function Naming

explicitly define what the function does.

## Casing

camelCase.

abbreviations are lowercase unless appended to another word, in which case full caps.

## Typings

inline unless very large. top of file.

## Export Policy

Litmus test for exporting vs importing; Would you fix a bug in this exported symbol for a stranger, and never rename it without a major version bump? If no, lowercase it or move it to /internal.
