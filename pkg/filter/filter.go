package filter

// KeyFilter defines the interface for probabilistic filters that check key membership
type KeyFilter interface {
	// Contains returns whether the key might be in the filter
	Contains(key string) bool
}
