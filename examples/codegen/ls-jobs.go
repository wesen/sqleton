package codegen

import (
	"context"
	sql "github.com/go-go-golems/clay/pkg/sql"
	cmds "github.com/go-go-golems/glazed/pkg/cmds"
	parameters "github.com/go-go-golems/glazed/pkg/cmds/parameters"
	maps "github.com/go-go-golems/glazed/pkg/helpers/maps"
	middlewares "github.com/go-go-golems/glazed/pkg/middlewares"
	sqlx "github.com/jmoiron/sqlx"
	"time"
)

const lsJobsCommandQuery = "SELECT\n  job_id,\n  status,\n  job_name,\n  last_updated_at\nFROM\n  jobs\nWHERE\n  1 = 1\n{{ if .from }}\n  AND last_updated_at >= '{{ .from }}'\n{{ end }}\n{{ if .to }}\n  AND last_updated_at <= '{{ .to }}'\n{{ end }}\n{{ if .status }}\n  AND status IN ({{ .status | sqlStringIn }})\n{{ end }}\nORDER BY\n  {{ .order_by }}\n{{ if .limit }}\n  LIMIT {{ .limit }}\n{{ end }}\n{{ if .offset }}\n  OFFSET {{ .offset }}\n{{ end }}\n"

type LsJobsCommand struct {
	*cmds.CommandDescription
	Query      string            `yaml:"query"`
	SubQueries map[string]string `yaml:"subqueries,omitempty"`
}

type LsJobsCommandParameters struct {
	From    time.Time `glazed.parameter:"from"`
	To      time.Time `glazed.parameter:"to"`
	Status  []string  `glazed.parameter:"status"`
	OrderBy string    `glazed.parameter:"order_by"`
	Limit   int       `glazed.parameter:"limit"`
	Offset  int       `glazed.parameter:"offset"`
}

func (p *LsJobsCommand) RunIntoGlazed(ctx context.Context, db *sqlx.DB, params *LsJobsCommandParameters, gp middlewares.Processor) error {
	ps := maps.StructToMap(params, false)
	renderedQuery, err := sql.RenderQuery(ctx, db, p.Query, p.SubQueries, ps)
	if err != nil {
		return err
	}

	err = sql.RunQueryIntoGlaze(ctx, db, renderedQuery, []interface{}{}, gp)
	if err != nil {
		return err
	}
	return nil
}

func NewLsJobsCommand() (*LsJobsCommand, error) {
	var flagDefs = []*parameters.ParameterDefinition{{
		Help: "Start date",
		Name: "from",
		Type: "date",
	}, {
		Help: "End date",
		Name: "to",
		Type: "date",
	}, {
		Help: "Status",
		Name: "status",
		Type: "stringList",
	}}

	var argDefs = []*parameters.ParameterDefinition{}

	cmdDescription := cmds.NewCommandDescription("ls-jobs",
		cmds.WithShort("List jobs"),
		cmds.WithLong(""),
		cmds.WithFlags(flagDefs...),
		cmds.WithArguments(argDefs...))

	return &LsJobsCommand{
		CommandDescription: cmdDescription,
		Query:              lsJobsCommandQuery,
		SubQueries:         map[string]string{},
	}, nil
}
