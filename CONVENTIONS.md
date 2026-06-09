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

## Pointers

don't explicitly use them unless you need them. only ingest/return them if you're okay with the consumer modifying them at their own discretion. return copy otherwise if needed.

## Context

dont store values in contexts.

use contexts where cancellation logic is applicable and preferable.

use TODO() when necessary.
