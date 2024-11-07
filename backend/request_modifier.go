package backend

import "regexp"

// RequestModifier is a function type that transforms a query string
type RequestModifier func(string, *[]ResultModifier) string

// Precompile regex for performance
var autoIncrementRegex = regexp.MustCompile(`(?i)AUTO_INCREMENT=\d+`)
var showSlaveStatusRegex = regexp.MustCompile(`(?i)^show\s+slave\s+status\s*;?$`)

// default request modifier list
var defaultRequestModifiers = []RequestModifier{
	replaceAutoIncrement,
	replaceShowSlaveStatus,
}

func replaceAutoIncrement(query string, modifiers *[]ResultModifier) string {
	if autoIncrementRegex.MatchString(query) {
		query = autoIncrementRegex.ReplaceAllString(query, "")
	}
	return query
}

func replaceShowSlaveStatus(query string, modifiers *[]ResultModifier) string {
	if showSlaveStatusRegex.MatchString(query) {
		*modifiers = append(*modifiers, replaceShowSlaveStatusFieldNames)
		query = showSlaveStatusRegex.ReplaceAllString(query, "SHOW REPLICA STATUS;")
	}
	return query
}

// applyRequestModifiers applies request modifiers to a query
func applyRequestModifiers(query string, requestModifiers []RequestModifier) (string, []ResultModifier) {
	resultModifiers := make([]ResultModifier, 0)
	for _, modifier := range requestModifiers {
		query = modifier(query, &resultModifiers)
	}
	return query, resultModifiers
}
