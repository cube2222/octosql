package octosql

import (
	"context"
	"errors"
	"github.com/cube2222/octosql/parser/sqlparser"
)


func (e *OctosqlExecutor) handleDDLStatement(ctx context.Context, statement *sqlparser.DDL) error {
	if statement.Action == sqlparser.CreateDataSourceStr {
		specs := statement.CrateDatasourceSpecs
		sourceName, _ := specs.TypeName.(*sqlparser.SQLVal)
		if sourceName.Type == sqlparser.StrVal {
			//("RUN CREATE SERVER [%v] [%v]\n", statement.Table.Name, string(sourceName.Val))

		}
		// TODO: Handle invalid source names
		return nil
	}
	return errors.New("Unsupported DDL statement was used.")
}