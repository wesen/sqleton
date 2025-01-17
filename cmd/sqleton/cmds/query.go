package cmds

import (
	"context"
	"github.com/go-go-golems/clay/pkg/sql"
	"github.com/go-go-golems/glazed/pkg/cmds"
	"github.com/go-go-golems/glazed/pkg/cmds/layers"
	"github.com/go-go-golems/glazed/pkg/cmds/parameters"
	"github.com/go-go-golems/glazed/pkg/middlewares"
	"github.com/go-go-golems/glazed/pkg/settings"
	cmds2 "github.com/go-go-golems/sqleton/pkg/cmds"
	"github.com/jmoiron/sqlx"
)

type QueryCommand struct {
	dbConnectionFactory cmds2.DBConnectionFactory
	*cmds.CommandDescription
}

func NewQueryCommand(
	dbConnectionFactory cmds2.DBConnectionFactory,
	options ...cmds.CommandDescriptionOption,
) (*QueryCommand, error) {
	glazeParameterLayer, err := settings.NewGlazedParameterLayers()
	if err != nil {
		return nil, err
	}
	options_ := append([]cmds.CommandDescriptionOption{
		cmds.WithShort("Run a SQL query passed as a CLI argument"),
		cmds.WithArguments(parameters.NewParameterDefinition(
			"query",
			parameters.ParameterTypeString,
			parameters.WithHelp("The SQL query to run"),
			parameters.WithRequired(true),
		),
		),
		cmds.WithLayers(glazeParameterLayer),
	}, options...)

	return &QueryCommand{
		dbConnectionFactory: dbConnectionFactory,
		CommandDescription:  cmds.NewCommandDescription("query", options_...),
	}, nil
}

func (q *QueryCommand) Run(
	ctx context.Context,
	parsedLayers map[string]*layers.ParsedParameterLayer,
	ps map[string]interface{},
	gp middlewares.Processor,
) error {
	query := ps["query"].(string)

	db, err := q.dbConnectionFactory(parsedLayers)
	if err != nil {
		return err
	}
	defer func(db *sqlx.DB) {
		_ = db.Close()
	}(db)

	err = db.PingContext(ctx)
	if err != nil {
		return err
	}

	err = sql.RunNamedQueryIntoGlaze(ctx, db, query, map[string]interface{}{}, gp)
	if err != nil {
		return err
	}

	return nil
}
