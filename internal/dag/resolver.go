package dag

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// refPattern matches state references like $.step_name.field or $.step_name.field.nested
var refPattern = regexp.MustCompile(`^\$\.([a-zA-Z_][a-zA-Z0-9_]*)\.(.+)$`)

// ResolveParams walks a JSON params blob and replaces all $.step_name.field
// string references with actual values from the StateBag.
// Only names of the job's steps and of results already in the StateBag are treated
// as references, any other string of the same shape (e.g. a JSONPath like
// $.items.count in an assertion) is kept as is.
// Returns the resolved JSON or an error if a reference cannot be resolved.
func ResolveParams(raw json.RawMessage, steps map[string]*Step, state *StateBag) (json.RawMessage, error) {
	if len(raw) == 0 {
		return raw, nil
	}

	var value interface{}
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, fmt.Errorf("failed to parse params: %w", err)
	}

	resolved, err := resolveValue(value, steps, state)
	if err != nil {
		return nil, err
	}

	return json.Marshal(resolved)
}

// resolveValue recursively resolves references in any JSON value.
func resolveValue(v interface{}, steps map[string]*Step, state *StateBag) (interface{}, error) {
	switch val := v.(type) {
	case string:
		return resolveString(val, steps, state)
	case map[string]interface{}:
		return resolveMap(val, steps, state)
	case []interface{}:
		return resolveSlice(val, steps, state)
	default:
		return v, nil
	}
}

// resolveString resolves a single string value. If the entire string is a
// reference (e.g. "$.step.field"), it returns the raw value (preserving type).
// If the string contains a reference as part of a larger string, it's not
// resolved (references must be the entire value).
func resolveString(s string, steps map[string]*Step, state *StateBag) (interface{}, error) {
	m := refPattern.FindStringSubmatch(s)
	if m == nil {
		return s, nil
	}

	stepName := m[1]
	fieldPath := m[2]

	result := state.Get(stepName)
	if _, isStep := steps[stepName]; !isStep && result == nil {
		return s, nil
	}
	if result == nil {
		return nil, fmt.Errorf("reference $.%s.%s: step %q has no result", stepName, fieldPath, stepName)
	}
	if result.Data == nil {
		return nil, fmt.Errorf("reference $.%s.%s: step %q has no output data", stepName, fieldPath, stepName)
	}

	val, err := resolveFieldPath(result.Data, fieldPath)
	if err != nil {
		return nil, fmt.Errorf("reference $.%s.%s: %w", stepName, fieldPath, err)
	}
	return val, nil
}

// resolveFieldPath traverses nested maps using dot-separated path.
func resolveFieldPath(data map[string]interface{}, path string) (interface{}, error) {
	parts := strings.Split(path, ".")
	var current interface{} = data

	for i, part := range parts {
		m, ok := current.(map[string]interface{})
		if !ok {
			traversed := strings.Join(parts[:i], ".")
			return nil, fmt.Errorf("field %q is not an object (at %q)", part, traversed)
		}
		val, exists := m[part]
		if !exists {
			return nil, fmt.Errorf("field %q not found", strings.Join(parts[:i+1], "."))
		}
		current = val
	}

	return current, nil
}

// resolveMap resolves all values in a JSON object.
func resolveMap(m map[string]interface{}, steps map[string]*Step, state *StateBag) (map[string]interface{}, error) {
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		resolved, err := resolveValue(v, steps, state)
		if err != nil {
			return nil, err
		}
		out[k] = resolved
	}
	return out, nil
}

// resolveSlice resolves all elements in a JSON array.
func resolveSlice(arr []interface{}, steps map[string]*Step, state *StateBag) ([]interface{}, error) {
	out := make([]interface{}, len(arr))
	for i, v := range arr {
		resolved, err := resolveValue(v, steps, state)
		if err != nil {
			return nil, err
		}
		out[i] = resolved
	}
	return out, nil
}
