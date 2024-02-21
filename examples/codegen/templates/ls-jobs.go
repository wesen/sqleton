package templates

import (
	"context"
	"fmt"
	"github.com/go-go-golems/clay/pkg/sql"
	"github.com/go-go-golems/glazed/pkg/cmds"
	"github.com/go-go-golems/glazed/pkg/cmds/layers"
	"github.com/go-go-golems/glazed/pkg/cmds/parameters"
	"github.com/go-go-golems/glazed/pkg/helpers/maps"
	"github.com/go-go-golems/glazed/pkg/middlewares"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"time"
)

const lsJobsCommandQuery = `SELECT
  job_id,
  status,
  job_name,
  last_updated_at
FROM
  jobs
WHERE
  1 = 1
{{ if .from }}
  AND last_updated_at >= '{{ .from }}'
{{ end }}
{{ if .to }}
  AND last_updated_at <= '{{ .to }}'
{{ end }}
{{ if .status }}
  AND status IN ({{ .status | sqlStringIn }})
{{ end }}
ORDER BY
  {{ .order_by }}
{{ if .limit }}
  LIMIT {{ .limit }}
{{ end }}
{{ if .offset }}
  OFFSET {{ .offset }}
{{ end }}
`

type LsJobsCommand struct {
	*cmds.CommandDescription
	Query      string            `yaml:"query"`
	SubQueries map[string]string `yaml:"subqueries,omitempty"`
	db         *sqlx.DB
}

var _ cmds.GlazeCommand = &LsJobsCommand{}

type LsJobsCommandParameters struct {
	from     time.Time `glazed.parameter:"from"`
	to       time.Time `glazed.parameter:"to"`
	status   []string  `glazed.parameter:"status"`
	order_by string    `glazed.parameter:"order_by"`
	limit    int       `glazed.parameter:"limit"`
	offset   int       `glazed.parameter:"offset"`
}

func (p *LsJobsCommand) RunQueryIntoGlaze(
	ctx context.Context,
	params *LsJobsCommandParameters,
	gp middlewares.Processor,
) error {
	ps := maps.StructToMap(params, false)
	renderedQuery, err := sql.RenderQuery(ctx, p.db, p.Query, p.SubQueries, ps)
	if err != nil {
		return err
	}

	err = sql.RunQueryIntoGlaze(ctx, p.db, renderedQuery, []interface{}{}, gp)
	if err != nil {
		return err
	}
	return nil
}
func (s *LsJobsCommand) RunIntoGlazeProcessor(
	ctx context.Context,
	parsedLayers *layers.ParsedLayers,
	gp middlewares.Processor,
) error {
	if s.db == nil {
		return fmt.Errorf("dbConnectionFactory is not set")
	}

	err := s.db.PingContext(ctx)
	if err != nil {
		return errors.Wrapf(err, "Could not ping database")
	}

	settings := &LsJobsCommandParameters{}
	err = parsedLayers.InitializeStruct(layers.DefaultSlug, settings)
	if err != nil {
		return err
	}

	ps := maps.StructToMap(settings, false)
	renderedQuery, err := sql.RenderQuery(ctx, s.db, s.Query, s.SubQueries, ps)
	if err != nil {
		return err
	}

	printQuery := false
	if printQuery_, ok := parsedLayers.GetParameter("sql-helpers", "print-query"); ok {
		printQuery = printQuery_.Value.(bool)
	}

	if printQuery {
		fmt.Println(renderedQuery)
		return &cmds.ExitWithoutGlazeError{}
	}

	err = s.RunQueryIntoGlaze(ctx, settings, gp)
	if err != nil {
		return errors.Wrapf(err, "Could not run query")
	}

	return nil
}
func NewLsJobsCommand(db *sqlx.DB) (*LsJobsCommand, error) {
	flagDefs := []*parameters.ParameterDefinition{
		parameters.NewParameterDefinition(
			"from",
			parameters.ParameterTypeDate,
			parameters.WithHelp("Start date"),
		),
		parameters.NewParameterDefinition(
			"to",
			parameters.ParameterTypeDate,
			parameters.WithHelp("End date"),
		),
		parameters.NewParameterDefinition(
			"status",
			parameters.ParameterTypeStringList,
			parameters.WithHelp("Status"),
		),
		parameters.NewParameterDefinition(
			"order_by",
			parameters.ParameterTypeString,
			parameters.WithHelp("Order by clause"),
			parameters.WithDefault("last_updated_at DESC")),
		parameters.NewParameterDefinition(
			"limit",
			parameters.ParameterTypeInteger,
			parameters.WithHelp("Limit clause (0 for no limit)"),
			parameters.WithDefault(50)),
		parameters.NewParameterDefinition(
			"offset",
			parameters.ParameterTypeInteger,
			parameters.WithHelp("Offset clause"),
		),
	}

	argDefs := []*parameters.ParameterDefinition{}

	cmdDescription := cmds.NewCommandDescription("LsJobs",
		cmds.WithShort("List jobs"),
		cmds.WithLong(""),
		cmds.WithFlags(flagDefs...),
		cmds.WithArguments(argDefs...),
	)

	return &LsJobsCommand{
		CommandDescription: cmdDescription,
		Query:              lsJobsCommandQuery,
		SubQueries:         map[string]string{},
		db:                 db,
	}, nil
}
