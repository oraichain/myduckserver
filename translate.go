// Copyright 2024-2025 ApeCloud, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"bytes"
	"fmt"
	"os/exec"
)

func getPythonPath() (string, error) {
	// Try to find python3 in the system PATH
	pythonPath, err := exec.LookPath("python3")
	if err == nil {
		return pythonPath, nil
	}

	// If python3 is not found, try to find python
	pythonPath, err = exec.LookPath("python")
	if err == nil {
		return pythonPath, nil
	}

	// If neither python3 nor python is found, return an error
	return "", fmt.Errorf("neither python3 nor python was found in PATH")
}

// translate converts a MySQL query to a DuckDB query using SQLGlot.
// For simplicity, we assume that Python and SQLGlot are installed on the system.
// Then we can call the following shell command to convert the query.
//
// python -c 'import sys; import sqlglot; sql = sys.stdin.read(); print(sqlglot.transpile(sql, read="mysql", write="duckdb")[0])
//
// In the future, we can deploy a SQLGlot server and use the API to convert the query.
func translate(mysqlQuery string) (string, error) {
	pythonPath, err := getPythonPath()
	if err != nil {
		fmt.Println("Error:", err)
		return "", err
	}

	// Prepare the command to be executed
	cmd := exec.Command(pythonPath, "-c", `import sys; import sqlglot; sql = sys.stdin.read(); print(sqlglot.transpile(sql, read="mysql", write="duckdb")[0])`)

	// Set the input for the command
	cmd.Stdin = bytes.NewBufferString(mysqlQuery)

	// Capture the output of the command
	var out bytes.Buffer
	var outErr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &outErr

	// Execute the command
	if err := cmd.Run(); err != nil {
		err = fmt.Errorf("failed to execute command(%s): %v\n%s", pythonPath, err, outErr.String())
		fmt.Println(err)
		return "", err
	}

	// Return the converted query
	return out.String(), nil
}
