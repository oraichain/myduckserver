package plugin

import "github.com/dolthub/go-mysql-server/sql/mysql_db"

var AuthPlugins = map[string]mysql_db.PlaintextAuthPlugin{
	"caching_sha2_password": &NoopPlaintextPlugin{},
}

// NoopPlaintextPlugin is used to authenticate plaintext user plugins
type NoopPlaintextPlugin struct{}

var _ mysql_db.PlaintextAuthPlugin = &NoopPlaintextPlugin{}

func (p *NoopPlaintextPlugin) Authenticate(db *mysql_db.MySQLDb, user string, userEntry *mysql_db.User, pass string) (bool, error) {
	return true, nil
}
