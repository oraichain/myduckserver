package mysqlutil

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// CauseImplicitCommitBefore returns true if the statement implicitly commits the current transaction:
// https://dev.mysql.com/doc/refman/8.4/en/implicit-commit.html
func CauseImplicitCommitBefore(node sql.Node) bool {
	switch node.(type) {
	case *plan.StartTransaction,
		*plan.LockTables, *plan.UnlockTables:
		return true
	default:
		return CauseImplicitCommitAfter(node)
	}
}

// CauseImplicitCommitAfter returns true if the statement cause an implicit commit after executing:
// https://dev.mysql.com/doc/refman/8.4/en/implicit-commit.html
func CauseImplicitCommitAfter(node sql.Node) bool {
	switch node.(type) {
	case *plan.CreateDB, *plan.DropDB, *plan.AlterDB,
		*plan.CreateTable, *plan.DropTable, *plan.RenameTable,
		*plan.AddColumn, *plan.RenameColumn, *plan.DropColumn, *plan.ModifyColumn, *plan.AlterDefaultSet, *plan.AlterDefaultDrop,
		*plan.Truncate,
		*plan.AnalyzeTable,
		*plan.CreateIndex, *plan.DropIndex, *plan.AlterIndex,
		*plan.CreateView, *plan.DropView,
		*plan.DropRole, *plan.CreateRole,
		*plan.AlterUser, *plan.CreateUser, *plan.DropUser, *plan.Grant, *plan.RenameUser,
		*plan.StartReplica, *plan.StopReplica, *plan.ResetReplica, *plan.ChangeReplicationSource:
		return true
	default:
		return false
	}
}

func CauseSchemaChange(node sql.Node) bool {
	switch node.(type) {
	case *plan.CreateDB, *plan.DropDB, *plan.AlterDB,
		*plan.CreateTable, *plan.DropTable, *plan.RenameTable,
		*plan.AddColumn, *plan.RenameColumn, *plan.DropColumn, *plan.ModifyColumn, *plan.AlterDefaultSet, *plan.AlterDefaultDrop,
		*plan.CreateIndex, *plan.DropIndex, *plan.AlterIndex,
		*plan.CreateView, *plan.DropView:
		return true
	default:
		return false
	}
}
