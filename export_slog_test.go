package event

// SlogDedupKeys returns the number of deduplication keys o currently tracks.
func SlogDedupKeys(o *SlogObserver) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.entries)
}
