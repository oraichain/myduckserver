// Copyright 2023 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package binlogreplication

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
)

const binlogPositionDirectory = ".replica"
const binlogPositionFilename = "binlog-position"
const mysqlFlavor = "MySQL56"

// binlogPositionStore manages loading and saving data to the binlog position file stored on disk. This provides
// durable storage for the set of GTIDs that have been successfully executed on the replica, so that the replica
// server can be restarted and resume binlog event messages at the correct point.
type binlogPositionStore struct {
	mu sync.Mutex
}

// Load loads a mysql.Position instance from the .replica/binlog-position file at the root of working directory
// This file MUST be stored at the root of the provider's filesystem, and NOT inside a nested database's .replica directory,
// since the binlog position contains events that cover all databases in a SQL server. The returned mysql.Position
// represents the set of GTIDs that have been successfully executed and applied on this replica. Currently only the
// default binlog channel ("") is supported. If no .replica/binlog-position file is stored, this method returns a nil
// mysql.Position and a nil error. If any errors are encountered, a nil mysql.Position and an error are returned.
func (store *binlogPositionStore) Load() (*mysql.Position, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	_, err := os.Stat(binlogPositionDirectory)
	if err != nil && errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	_, err = os.Stat(filepath.Join(binlogPositionDirectory, binlogPositionFilename))
	if err != nil && errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	filePath, err := filepath.Abs(filepath.Join(binlogPositionDirectory, binlogPositionFilename))
	if err != nil {
		return nil, err
	}

	bytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	positionString := string(bytes)

	// Strip off the "MySQL56/" prefix
	prefix := "MySQL56/"
	if strings.HasPrefix(positionString, prefix) {
		positionString = string(bytes[len(prefix):])
	}

	position, err := mysql.ParsePosition(mysqlFlavor, positionString)
	if err != nil {
		return nil, err
	}

	return &position, nil
}

// Save saves the specified |position| to disk in the .replica/binlog-position file at the root of the provider's
// filesystem. This file MUST be stored at the root of the provider's filesystem, and NOT inside a nested database's
// .replica directory, since the binlog position contains events that cover all databases in a SQL server. |position|
// represents the set of GTIDs that have been successfully executed and applied on this replica. Currently only the
// default binlog channel ("") is supported. If any errors are encountered persisting the position to disk, an
// error is returned.
func (store *binlogPositionStore) Save(ctx *sql.Context, engine *gms.Engine, position *mysql.Position) error {
	if position == nil {
		return fmt.Errorf("unable to save binlog position: nil position passed")
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	// The .replica dir may not exist yet, so create it if necessary.
	dir, err := createReplicaDir(engine)
	if err != nil {
		return err
	}

	filePath, err := filepath.Abs(filepath.Join(dir, binlogPositionFilename))
	if err != nil {
		return err
	}

	encodedPosition := mysql.EncodePosition(*position)
	return os.WriteFile(filePath, []byte(encodedPosition), 0666)
}

// Delete deletes the stored mysql.Position information stored in .replica/binlog-position in the root of the provider's
// filesystem. This is useful for the "RESET REPLICA" command, since it clears out the current replication state. If
// any errors are encountered removing the position file, an error is returned.
func (store *binlogPositionStore) Delete(ctx *sql.Context) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	return os.Remove(filepath.Join(binlogPositionDirectory, binlogPositionFilename))
}

// createReplicaDir creates the .replica directory if it doesn't already exist.
func createReplicaDir(engine *gms.Engine) (string, error) {
	dir := filepath.Join(getDataDir(engine), binlogPositionDirectory)
	stat, err := os.Stat(dir)
	if err != nil && errors.Is(err, fs.ErrNotExist) {
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return "", fmt.Errorf("unable to save binlog metadata: %s", err)
		}
	} else if err != nil {
		return "", err
	} else if !stat.IsDir() {
		return "", fmt.Errorf("unable to save binlog metadata: %s exists as a file, not a dir", binlogPositionDirectory)
	}

	return dir, nil
}
