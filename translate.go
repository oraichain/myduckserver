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
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os/exec"
	"strings"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

const (
	cmdExit = "CMD:EXIT"
	cmdRun  = "CMD:RUN"

	resultOK  = "OK:"
	resultErr = "ERROR:"
)

var (
	errPythonProcessUnhealthy = errors.NewKind("sqlglot python process is unhealthy: %s")
)

type translationRequest struct {
	cmd string
	sql string
}

type translationResponse struct {
	result string
	err    error
}

type translateService struct {
	requestChan  chan translationRequest
	responseChan chan translationResponse
	pythonStdin  io.WriteCloser
	pythonCmd    *exec.Cmd
	stderrBuf    *bytes.Buffer
}

var translationSvc *translateService

func newTranslateService() (*translateService, error) {
	pythonPath, err := getPythonPath()
	if err != nil {
		return nil, fmt.Errorf("failed to get Python path: %v", err)
	}

	pythonScript := fmt.Sprintf(`
import sys
import sqlglot

CMD_EXIT = %q
CMD_RUN = %q
RESULT_OK = %q
RESULT_ERR = %q

def read_bytes(n: int):
    bytes = b''
    while n > 0:
        reads = sys.stdin.buffer.read(n)
        if len(reads) == 0:
            # The stdin has been closed, indicating that the parent process has exited.
            # We should exit the child process to prevent orphan Python processes.
            raise EOFError("EOF")
        bytes += reads
        n -= len(reads)
    return bytes

def read_string():
    data = read_bytes(4)
    length = int.from_bytes(data, byteorder='big')
    data = read_bytes(length)
    return data.decode('utf-8')

def write_string(s: str):
    data = s.encode('utf-8')
    # write the length of the string first
    sys.stdout.buffer.write(len(data).to_bytes(4, byteorder='big'))
    sys.stdout.buffer.write(data)
    sys.stdout.flush()

while True:
    inp = read_string()
    if inp == CMD_EXIT:
        break
    if inp.startswith(CMD_RUN):
        sql = inp[len(CMD_RUN):]
        try:
            result = sqlglot.transpile(sql, read="mysql", write="duckdb")[0]
            write_string(RESULT_OK + result)
        except Exception as e:
            write_string(RESULT_ERR + str(e))
`, cmdExit, cmdRun, resultOK, resultErr)

	pythonCmd := exec.Command(pythonPath, "-u", "-c", pythonScript)

	pythonStdin, err := pythonCmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stdin pipe: %v", err)
	}

	stdout, err := pythonCmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stdout pipe: %v", err)
	}

	var stderrBuf bytes.Buffer
	pythonCmd.Stderr = &stderrBuf

	err = pythonCmd.Start()
	if err != nil {
		return nil, fmt.Errorf("failed to start Python process: %v", err)
	}

	requestChan := make(chan translationRequest)
	responseChan := make(chan translationResponse)

	svc := &translateService{
		requestChan:  requestChan,
		responseChan: responseChan,
		pythonStdin:  pythonStdin,
		pythonCmd:    pythonCmd,
		stderrBuf:    &stderrBuf,
	}
	go svc.handleTranslations(pythonStdin, bufio.NewReader(stdout))

	// Test the translation service with a simple query
	testSQL := "SELECT 1"
	translatedSQL, err := svc.translate(testSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to test translation service: %v", err)
	}
	if translatedSQL != "SELECT 1" {
		return nil, fmt.Errorf("unexpected translation result: %s", translatedSQL)
	}

	return svc, nil
}

func (svc *translateService) translate(sql string) (string, error) {
	svc.requestChan <- translationRequest{cmd: cmdRun, sql: sql}
	response := <-svc.responseChan

	if errors.Is(response.err, errPythonProcessUnhealthy) {
		svc.cleanup()
		panic(fmt.Errorf("%v\ncmd:\n%s\nstderr:\n%s", response.err, svc.pythonCmd.String(), svc.stderrBuf.String()))
	}

	return response.result, response.err
}

func (svc *translateService) handleTranslations(stdin io.Writer, stdout *bufio.Reader) {
	for req := range svc.requestChan {
		if req.cmd == cmdExit {
			sendString(stdin, cmdExit)
			break
		}

		err := sendString(stdin, cmdRun+req.sql)
		if err != nil {
			svc.responseChan <- translationResponse{"", errPythonProcessUnhealthy.New(err)}
			continue
		}

		result, err := recvString(stdout)
		if err != nil {
			svc.responseChan <- translationResponse{"", errPythonProcessUnhealthy.New(err)}
			continue
		}

		result = strings.TrimSpace(result)

		if strings.HasPrefix(result, resultErr) {
			svc.responseChan <- translationResponse{"", fmt.Errorf(result[len(resultErr):])}
		} else if strings.HasPrefix(result, resultOK) {
			svc.responseChan <- translationResponse{strings.TrimSpace(result[len(resultOK):]), nil}
		} else {
			svc.responseChan <- translationResponse{"", fmt.Errorf("unexpected result: %s", result)}
		}
	}
}

// the schema is 4 bytes length + data
func sendString(writer io.Writer, str string) error {
	data := []byte(str)
	length := len(data)
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, uint32(length))
	_, err := writer.Write(lengthBytes)
	if err != nil {
		return err
	}
	_, err = writer.Write(data)
	return err
}

func recvString(reader *bufio.Reader) (string, error) {
	lengthBytes := make([]byte, 4)
	_, err := io.ReadFull(reader, lengthBytes)
	if err != nil {
		return "", err
	}
	length := binary.BigEndian.Uint32(lengthBytes)
	data := make([]byte, length)
	_, err = io.ReadFull(reader, data)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (svc *translateService) cleanup() {
	svc.requestChan <- translationRequest{cmd: cmdExit}
	svc.pythonCmd.Wait()
	close(svc.requestChan)
	close(svc.responseChan)
}

func startTranslationService() error {
	svc, err := newTranslateService()
	if err != nil {
		return fmt.Errorf("failed to initialize translation service: %v", err)
	}
	translationSvc = svc
	return nil
}

func stopTranslationService() {
	translationSvc.cleanup()
}

func translate(node sql.Node, sql string) (string, error) {
	switch node.(type) {
	case *plan.CreateTable,
		// Convert the CREATE TABLE statement using the built-in transpiler; ignore possible create index statements for now
		*plan.ResolvedTable,
		// Simple SELECT statements, e.g., `SELECT * FROM tbl` or `SELECT col1, col2 FROM tbl`
		*plan.ShowTables,
		*plan.ShowColumns:
		return translateBuiltIn(sql)
	default:
		// For other types of queries, use SQLGlot to convert the query
		return translateWithSQLGlot(sql)
	}
}

func translateBuiltIn(sql string) (string, error) {
	// TODO(fan): https://github.com/dolthub/doltgresql/issues/660
	// return transpiler.ConvertQuery(sql)[0], nil
	return translateWithSQLGlot(sql)
}

func translateWithSQLGlot(sql string) (string, error) {
	if translationSvc == nil {
		return "", fmt.Errorf("translation service is not initialized")
	}

	return translationSvc.translate(sql)
}

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
