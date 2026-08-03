package jsontype

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
)

// renderState threads the options through a render and collects the imports
// the finished expression needs.
type renderState struct {
	opts    Options
	imports map[string]bool
}

// goType renders the narrowest Go type that holds everything v observed.
// observations is how many times v's parent was seen, which is what decides
// optionality: a key absent from some observations earns an omitempty — unless
// it was ever empty on the wire, when the tag would drop it — and a null one
// earns a pointer the caller has to check.
func (v *value) goType(observations int, state *renderState) (expr string, omitEmpty bool) {
	if v == nil {
		return "any", false
	}
	distinct := 0
	for _, n := range [...]int{v.arrays, v.bools, v.float64s, v.ints, v.nulls, v.objects, v.strings} {
		if n > 0 {
			distinct++
		}
	}

	switch {
	case distinct == 1 && v.arrays > 0,
		distinct == 2 && v.arrays > 0 && v.nulls > 0:
		elem, _ := v.elems.goType(0, state)
		return "[]" + elem, v.arrays+v.nulls < observations && v.empties == 0
	case distinct == 1 && v.bools > 0:
		return "bool", v.bools < observations && v.empties == 0
	case distinct == 2 && v.bools > 0 && v.nulls > 0:
		return "*bool", false
	case distinct == 1 && v.ints > 0:
		return "int64", v.ints < observations && v.empties == 0
	case distinct == 2 && v.ints > 0 && v.nulls > 0:
		return "*int64", false
	// A number that was whole in one observation and fractional in another is
	// one numeric field, so int64 widens to float64 rather than to any.
	case distinct == 1 && v.float64s > 0,
		distinct == 2 && v.float64s > 0 && v.ints > 0:
		return "float64", v.float64s+v.ints < observations && v.empties == 0
	case distinct == 2 && v.float64s > 0 && v.nulls > 0,
		distinct == 3 && v.float64s > 0 && v.ints > 0 && v.nulls > 0:
		return "*float64", false
	case distinct == 1 && v.objects > 0,
		distinct == 2 && v.objects > 0 && v.nulls > 0:
		return v.objectType(observations, state)
	case distinct == 1 && v.strings > 0 && v.times == v.strings:
		state.imports["time"] = true
		return "time.Time", v.times < observations
	case distinct == 1 && v.strings > 0:
		return "string", v.strings < observations && v.empties == 0
	case distinct == 2 && v.strings > 0 && v.nulls > 0 && v.times == v.strings:
		state.imports["time"] = true
		return "*time.Time", false
	case distinct == 2 && v.strings > 0 && v.nulls > 0:
		return "*string", false
	default:
		// Observations that agree on nothing — or a bare null, which reveals
		// nothing — can only be read as any.
		return "any", v.arrays+v.bools+v.float64s+v.ints+v.nulls+v.objects+v.strings < observations
	}
}

// objectType renders an observed object: a struct, the map that models a
// dictionary, or the pointer to either when some observations went without it.
func (v *value) objectType(observations int, state *renderState) (string, bool) {
	if len(v.fields) == 0 {
		switch {
		case observations == 0 && v.nulls == 0:
			return "struct{}", false
		case v.nulls > 0:
			return "*struct{}", false
		case v.objects == observations:
			return "struct{}", false
		default:
			return "*struct{}", v.objects < observations
		}
	}

	expr := v.structType(state)
	// The pointer and optionality rules are the object's regardless of whether
	// its expression collapsed to a map: what was observed either appeared in
	// every observation or it did not.
	switch {
	case observations == 0, v.objects == observations:
		return expr, false
	case v.nulls == 0:
		return "*" + expr, true
	default:
		return "*" + expr, v.objects+v.nulls < observations
	}
}

// structType renders the struct declaring an object's fields, or the map that
// models it when the keys read as data rather than names.
func (v *value) structType(state *renderState) string {
	keys := v.keys
	if !state.opts.KeepOrder {
		keys = slices.Sorted(maps.Keys(v.fields))
	}

	var b strings.Builder
	b.WriteString("struct {\n")
	taken := make(map[string]bool, len(keys))
	var fieldTypes []string
	for _, key := range keys {
		// A key encoding/json cannot bind — empty, or holding a character the
		// tag grammar reserves — gets no field: one would silently read back
		// under the field's own name instead.
		if key == "" || strings.ContainsAny(key, ` ",`) {
			continue
		}
		fieldType, omitEmpty := v.fields[key].goType(v.objects, state)
		name := uniqueFieldName(exportName(key), taken)
		taken[name] = true
		tag := key
		if omitEmpty {
			tag += ",omitempty"
		}
		fmt.Fprintf(&b, "%s %s `json:%s`\n", name, fieldType, strconv.Quote(tag))
		fieldTypes = append(fieldTypes, fieldType)
	}
	b.WriteString("}")

	if min := state.opts.DictionaryFields; min > 0 && len(fieldTypes) >= min && uniform(fieldTypes) {
		return "map[string]" + fieldTypes[0]
	}
	return b.String()
}

// uniform reports whether every rendered field type is the same one, which is
// what lets a wide object read as a lookup table.
func uniform(types []string) bool {
	for _, t := range types[1:] {
		if t != types[0] {
			return false
		}
	}
	return true
}
