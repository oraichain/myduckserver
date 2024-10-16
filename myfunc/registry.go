package myfunc

import "github.com/dolthub/go-mysql-server/sql"

var ExtraBuiltIns = []sql.Function{
	sql.Function0{Name: "ps_current_thread_id", Fn: NewPSCurrentThreadID},
}
