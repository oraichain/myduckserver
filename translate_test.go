package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestTranslate(t *testing.T) {
	// Define a slice of test cases
	testCases := []struct {
		name     string
		input    string
		expected string
	}{

		// fixme: it seems AUTO_INCREMENT is not supported by duckdb
		{
			name:     "Simple CREATE TABLE with AUTO_INCREMENT",
			input:    "CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY)",
			expected: "CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY)",
		},
		{
			name:     "CREATE TABLE without AUTO_INCREMENT",
			input:    "CREATE TABLE orders (order_id INT PRIMARY KEY)",
			expected: "CREATE TABLE orders (order_id INT PRIMARY KEY)",
		},
		{
			name:     "CREATE TABLE with multiple columns",
			input:    "CREATE TABLE products (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255))",
			expected: "CREATE TABLE products (id INT AUTO_INCREMENT PRIMARY KEY, name TEXT(255))",
		},
		{
			name:     "CREATE TABLE with foreign key",
			input:    "CREATE TABLE orders (order_id INT PRIMARY KEY, user_id INT, FOREIGN KEY (user_id) REFERENCES users(id))",
			expected: "CREATE TABLE orders (order_id INT PRIMARY KEY, user_id INT, FOREIGN KEY (user_id) REFERENCES users (id))",
		},
		// Add more test cases as needed
	}

	// Loop over each test case
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := translate(tc.input)
			if err != nil {
				t.Errorf("translate(%q) returned an error: %v", tc.input, err)
			}

			trimmedResult := strings.TrimSpace(result)
			fmt.Println("trimmedResult:", trimmedResult)
			if trimmedResult != tc.expected {
				t.Errorf("translate(%q) = %v; want %v", tc.input, trimmedResult, tc.expected)
			}
		})
	}
}
