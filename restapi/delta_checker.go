package restapi

import (
	"reflect"
	"strings"
)

/*
 * Performs a deep comparison of two maps - the resource as recorded in state, and the resource as returned by the API.
 * Accepts a third argument that is a set of fields that are to be ignored when looking for differences.
 *
 * Supports three ignore pattern types:
 * 1. Wildcard patterns (e.g., "*.metadata"): Match at any nesting level
 * 2. Simple keys without dots (e.g., "metadata"): Match only at root level
 * 3. Dotted paths (e.g., "resource.connectorRef.oid"): Match at specific path
 *
 * Returns 1. the recordedResource overlaid with fields that have been modified in actualResource but not ignored, and 2. a bool true if there were any changes.
 */
func getDelta(recordedResource map[string]interface{}, actualResource map[string]interface{}, ignoreList []string) (modifiedResource map[string]interface{}, hasChanges bool) {
	modifiedResource = map[string]interface{}{}
	hasChanges = false

	// Keep track of keys we've already checked in actualResource to reduce work when checking keys in actualResource
	checkedKeys := map[string]struct{}{}

	for key, valRecorded := range recordedResource {

		checkedKeys[key] = struct{}{}

		// If the ignore_list contains the current key, don't compare
		if matchesIgnorePattern(key, ignoreList) {
			modifiedResource[key] = valRecorded
			continue
		}

		valActual, actualHasKey := actualResource[key]

		if valRecorded == nil {
			// A JSON null was put in input data, confirm the result is either not set or is also null
			modifiedResource[key] = valActual
			if actualHasKey && valActual != nil {
				hasChanges = true
			}
		} else if reflect.TypeOf(valRecorded).Kind() == reflect.Map {
			// If valRecorded was a map, assert both values are maps
			subMapA, okA := valRecorded.(map[string]interface{})
			subMapB, okB := valActual.(map[string]interface{})
			if !okA || !okB {
				modifiedResource[key] = valActual
				hasChanges = true
				continue
			}
			// Recursively compare
			deeperIgnoreList := _descendIgnoreList(key, ignoreList)
			if modifiedSubResource, hasChange := getDelta(subMapA, subMapB, deeperIgnoreList); hasChange {
				modifiedResource[key] = modifiedSubResource
				hasChanges = true
			} else {
				modifiedResource[key] = valRecorded
			}
		} else if reflect.TypeOf(valRecorded).Kind() == reflect.Slice {
			// Handle arrays by comparing elements recursively if they contain maps
			sliceRecorded, okRecorded := valRecorded.([]interface{})
			sliceActual, okActual := valActual.([]interface{})

			// Try casting to []map[string]interface{} if []interface{} cast fails
			if !okRecorded || !okActual {
				sliceMapRecorded, okMapRecorded := valRecorded.([]map[string]interface{})
				sliceMapActual, okMapActual := valActual.([]map[string]interface{})

				if okMapRecorded && okMapActual {
					// Convert []map[string]interface{} to []interface{}
					sliceRecorded = make([]interface{}, len(sliceMapRecorded))
					sliceActual = make([]interface{}, len(sliceMapActual))
					for i, m := range sliceMapRecorded {
						sliceRecorded[i] = m
					}
					for i, m := range sliceMapActual {
						sliceActual[i] = m
					}
					okRecorded = true
					okActual = true
				} else {
					// Can't cast to either type, fall back to DeepEqual
					if !reflect.DeepEqual(valRecorded, valActual) {
						modifiedResource[key] = valActual
						hasChanges = true
					} else {
						modifiedResource[key] = valRecorded
					}
				}
			}

			if okRecorded && okActual && len(sliceRecorded) != len(sliceActual) {
				// Different array lengths means there's a change
				modifiedResource[key] = valActual
				hasChanges = true
			} else if okRecorded && okActual {
				// Same length, compare elements
				// Descend ignore list for array elements (propagate wildcards)
				deeperIgnoreList := _descendIgnoreList(key, ignoreList)
				modifiedSlice := make([]interface{}, len(sliceRecorded))
				sliceHasChanges := false

				for i := 0; i < len(sliceRecorded); i++ {
					elemRecorded := sliceRecorded[i]
					elemActual := sliceActual[i]

					// If element is a map, recursively compare with descended ignore list
					if reflect.TypeOf(elemRecorded).Kind() == reflect.Map {
						mapRecorded, okRecorded := elemRecorded.(map[string]interface{})
						mapActual, okActual := elemActual.(map[string]interface{})

						if okRecorded && okActual {
							// Recursively compare maps within the array with descended ignore list
							if modifiedElem, elemChanged := getDelta(mapRecorded, mapActual, deeperIgnoreList); elemChanged {
								modifiedSlice[i] = modifiedElem
								sliceHasChanges = true
							} else {
								modifiedSlice[i] = elemRecorded
							}
						} else {
							// Can't cast to maps, use DeepEqual
							if !reflect.DeepEqual(elemRecorded, elemActual) {
								modifiedSlice[i] = elemActual
								sliceHasChanges = true
							} else {
								modifiedSlice[i] = elemRecorded
							}
						}
					} else {
						// For non-map elements (strings, numbers, etc.), use DeepEqual
						if !reflect.DeepEqual(elemRecorded, elemActual) {
							modifiedSlice[i] = elemActual
							sliceHasChanges = true
						} else {
							modifiedSlice[i] = elemRecorded
						}
					}
				}

				if sliceHasChanges {
					modifiedResource[key] = modifiedSlice
					hasChanges = true
				} else {
					modifiedResource[key] = valRecorded
				}
			}
		} else if valRecorded != valActual {
			modifiedResource[key] = valActual
			hasChanges = true
		} else {
			// In this case, the recorded and actual values were the same
			modifiedResource[key] = valRecorded
		}

	}

	for key, valActual := range actualResource {
		// We may have already compared this key with recordedResource
		_, alreadyChecked := checkedKeys[key]
		if alreadyChecked {
			continue
		}

		// If the ignore_list contains the current key, don't compare.
		// Don't modify modifiedResource either - we don't want this key to be tracked
		if matchesIgnorePattern(key, ignoreList) {
			continue
		}

		// If we've gotten here, that means actualResource has an additional key that wasn't in recordedResource
		modifiedResource[key] = valActual
		hasChanges = true
	}

	return modifiedResource, hasChanges
}

/*
 * Modifies an ignoreList to be relative to a descended path.
 * E.g. given descendPath = "bar", and the ignoreList [foo, bar.alpha, bar.bravo], this returns [alpha, bravo]
 *
 * Supports three pattern types:
 * 1. Wildcard patterns (e.g., "*.metadata"): Propagated to all nested levels for recursive matching
 * 2. Simple keys without dots (e.g., "metadata"): Only match at root level, NOT propagated
 * 3. Dotted paths (e.g., "resource.connectorRef.oid"): Only match at specific paths
 */
func _descendIgnoreList(descendPath string, ignoreList []string) []string {
	newIgnoreList := make([]string, 0, len(ignoreList))

	for _, ignorePath := range ignoreList {
		// Wildcard patterns (*.field) are propagated recursively to all levels
		if strings.HasPrefix(ignorePath, "*.") {
			newIgnoreList = append(newIgnoreList, ignorePath)
			continue
		}

		pathComponents := strings.Split(ignorePath, ".")

		// Simple keys without dots only match at root level - do NOT propagate
		if len(pathComponents) == 1 {
			// Don't add to newIgnoreList - this key only matches at the current level
			continue
		}

		// For dotted paths, descend if the first component matches
		if pathComponents[0] == descendPath {
			// If this ignorePath starts with the descendPath, remove the first component and keep the rest
			modifiedPath := strings.Join(pathComponents[1:], ".")
			newIgnoreList = append(newIgnoreList, modifiedPath)
		}
	}

	return newIgnoreList
}

func contains(list []string, elem string) bool {
	for _, a := range list {
		if a == elem {
			return true
		}
	}
	return false
}

/*
 * matchesIgnorePattern checks if a field name matches any pattern in the ignore list.
 * Supports two pattern types:
 * 1. Exact match: "fieldname" matches only "fieldname"
 * 2. Wildcard match: "*.fieldname" matches "fieldname" at any level
 */
func matchesIgnorePattern(fieldName string, ignoreList []string) bool {
	for _, pattern := range ignoreList {
		// Check for exact match
		if pattern == fieldName {
			return true
		}
		// Check for wildcard match (pattern starts with "*.")
		if strings.HasPrefix(pattern, "*.") {
			wildcardField := pattern[2:] // Remove "*." prefix
			if wildcardField == fieldName {
				return true
			}
		}
	}
	return false
}

/*
 * filterIgnoredFields recursively removes fields from a map that match patterns in the ignore list.
 * This is used to remove server-managed fields from input JSON before sending to the API.
 *
 * Supports three pattern types:
 * 1. Wildcard patterns (e.g., "*.metadata"): Filtered recursively at all levels
 * 2. Simple keys without dots (e.g., "metadata"): Only filtered at root level
 * 3. Dotted paths (e.g., "resource.connectorRef.oid"): Only filtered at the specific path
 */
func filterIgnoredFields(data map[string]interface{}, ignoreList []string) map[string]interface{} {
	if data == nil {
		return nil
	}

	result := make(map[string]interface{})

	for key, value := range data {
		// Skip this key if it's in the ignore list
		if matchesIgnorePattern(key, ignoreList) {
			continue
		}

		// Check if this is a map - if so, recurse with the descended ignore list
		if mapValue, ok := value.(map[string]interface{}); ok {
			descendedIgnoreList := _descendIgnoreList(key, ignoreList)
			result[key] = filterIgnoredFields(mapValue, descendedIgnoreList)
		} else if sliceValue, ok := value.([]interface{}); ok {
			// Handle arrays by recursively filtering map elements
			// Descend ignore list for array elements (propagate wildcards)
			descendedIgnoreList := _descendIgnoreList(key, ignoreList)
			filteredSlice := make([]interface{}, len(sliceValue))
			for i, elem := range sliceValue {
				if mapElem, ok := elem.(map[string]interface{}); ok {
					// Recursively filter maps within the array using descended ignore list
					filteredSlice[i] = filterIgnoredFields(mapElem, descendedIgnoreList)
				} else {
					// For non-map elements, keep them as-is
					filteredSlice[i] = elem
				}
			}
			result[key] = filteredSlice
		} else {
			// For primitive values, keep them as-is
			result[key] = value
		}
	}

	return result
}
