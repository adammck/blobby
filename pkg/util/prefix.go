package util

// IncrementPrefix calculates the next lexicographic string after the given prefix
// for use in range scans. It handles UTF-8 properly by incrementing bytes.
//
// This is useful for converting prefix scans to range scans by finding the
// exclusive upper bound for a prefix. For example:
//   - prefix "abc" becomes range ["abc", "abd")
//   - prefix "ab\xff" becomes range ["ab\xff", "ac")
//   - prefix "\xff\xff" becomes unbounded range ["\xff\xff", "")
func IncrementPrefix(prefix string) string {
	if prefix == "" {
		return ""
	}

	// Convert to bytes for manipulation
	bytes := []byte(prefix)

	// Find the last byte that can be incremented
	for i := len(bytes) - 1; i >= 0; i-- {
		if bytes[i] < 0xff {
			bytes[i]++
			// Truncate any trailing bytes that were 0xff
			return string(bytes[:i+1])
		}
	}

	// All bytes were 0xff, so there's no valid upper bound
	// Return empty string to indicate unbounded scan
	return ""
}