package harness

import (
	"testing"
)

func TestQuerySignature(t *testing.T) {
	testCases := []struct {
		input    string
		expected string
	}{
		{
			input:    "SELECT * FROM mytable t0 INNER JOIN mytable t1 ON (t1.i IN (((true)%(''))));",
			expected: "select_*_from_mytable_t0_inner_join_mytable_t1_on_(t1.i_in_(((true)%(''))));",
		},
		{
			input:    "select_*_from_mytable_t0_inner_join_mytable_t1_on_(t1.i_in_(((true)%(''))));",
			expected: "select_*_from_mytable_t0_inner_join_mytable_t1_on_(t1.i_in_(((true)%(''))));",
		},
		{
			input:    "SELECT 1 % true",
			expected: "select_1_%_true",
		},
		{
			input:    "SELECT 1 WHERE ((1 IN (NULL >= 1)) IS NULL);",
			expected: "select_1_where_((1_in_(null_>=_1))_is_null);",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			result := querySignature(tc.input)
			if result != tc.expected {
				t.Errorf("querySignature(%q) = %q; expected %q", tc.input, result, tc.expected)
			}
		})
	}
}
