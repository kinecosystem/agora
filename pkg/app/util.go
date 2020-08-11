package app

import "unicode"

// IsValidAppID returns whether or not the provided string is a valid app ID.
func IsValidAppID(appID string) bool {
	if len(appID) < 3 || len(appID) > 4 {
		return false
	}

	for _, r := range appID {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			return false
		}
	}

	return true
}
