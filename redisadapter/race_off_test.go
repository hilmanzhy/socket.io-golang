//go:build !race

package redisadapter

// raceEnabled reports whether the tests run under the race detector, which
// allocates on its own and so changes every count of allocations.
const raceEnabled = false
