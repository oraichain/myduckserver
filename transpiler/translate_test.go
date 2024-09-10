package transpiler

import (
	"fmt"
	"strings"
	"sync"
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
		{
			name:     "SELECT with newlines",
			input:    "SELECT '\n' FROM users WHERE id = 1\n",
			expected: "SELECT '\n' FROM users WHERE id = 1",
		},
		{
			name:     "multiple statements",
			input:    "CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY); \n SELECT * FROM users WHERE id = 1",
			expected: "CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY)",
		},
	}

	// Loop over each test case
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := TranslateWithSQLGlot(tc.input)
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

func TestTranslateConcurrent(t *testing.T) {
	concurrency := 1000
	wg := sync.WaitGroup{}
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func(i int) {
			defer wg.Done()

			sql := fmt.Sprintf("SELECT * FROM users WHERE id = %d", i)
			expectedSQL := sql

			translatedSQL, err := TranslateWithSQLGlot(sql)
			if err != nil {
				t.Errorf("translate(%q) returned an error: %v", sql, err)
			}
			if translatedSQL != expectedSQL {
				t.Errorf("translate(%q) = %v; want %v", sql, translatedSQL, expectedSQL)
			}
		}(i)
	}

	wg.Wait()
}
