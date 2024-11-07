package backend

import (
	"regexp"

	"github.com/dolthub/vitess/go/sqltypes"
)

// ResultModifier is a function type that transforms a Result
type ResultModifier func(*sqltypes.Result) *sqltypes.Result

var (
	replicaRegex = regexp.MustCompile(`^Replica_`)
	sourceRegex  = regexp.MustCompile(`Source`)
)

// replaceFieldNames modifies field names to maintain compatibility with older MySQL clients
// by replacing "Replica_" with "Slave_" and "Source" with "Master"
func replaceShowSlaveStatusFieldNames(result *sqltypes.Result) *sqltypes.Result {
	if result == nil || result.Fields == nil {
		return result
	}

	for i, field := range result.Fields {
		name := field.Name
		// Replace any "Replica_" with "Slave_"
		if replicaRegex.MatchString(name) {
			result.Fields[i].Name = replicaRegex.ReplaceAllString(name, "Slave_")
		}
		// Replace any "Source" with "Master"
		if sourceRegex.MatchString(name) {
			result.Fields[i].Name = sourceRegex.ReplaceAllString(name, "Master")
		}
	}
	return result
}
