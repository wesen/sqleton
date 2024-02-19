package templates

import (
	"bytes"
	cmds2 "github.com/go-go-golems/glazed/pkg/cmds"
	"github.com/go-go-golems/glazed/pkg/cmds/parameters"
	"github.com/go-go-golems/glazed/pkg/codegen"
	"github.com/go-go-golems/sqleton/pkg/cmds"
	"github.com/iancoleman/strcase"
	"strconv"
	"strings"
	"text/template"
)

const structTemplate = `type {{ .StructName }} struct {
	*cmds.CommandDescription
	Query      string            ` + "`yaml:\"query\"`" + `
	SubQueries map[string]string ` + "`yaml:\"subqueries,omitempty\"`" + `
    db *sqlx.DB
}

var _ cmds.GlazeCommand = &{{ .StructName }}{}
`
const parametersStructTemplate = `type {{ .StructName }} struct {
{{- range .Parameters }}
	{{ .Name }} {{ .Type | flagTypeToGoType }} ` + "`glazed.parameter:\"{{ .Name }}\"`" + `
{{- end }}
}
`
const runIntoGlazedTemplate = `func (p *{{ .Receiver }}) RunQueryIntoGlaze(
	ctx context.Context,
	params *{{ .ParametersStruct }},
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
`
const newFunctionTemplate = `func New{{ .CommandName }}Command(db *sqlx.DB) (*{{ .StructName }}, error) {
	flagDefs := []*parameters.ParameterDefinition{
{{- range .Flags }}
		parameters.NewParameterDefinition(
			"{{ .Name }}",
			{{ .Type | unquoteParameterType}} ,
			{{- if .ShortFlag }}parameters.WithShortFlag("{{ .ShortFlag }}"),{{ end }}
			parameters.WithHelp("{{ .Help }}"),
			{{ if .Default }}parameters.WithDefault({{ .Default | toGoCode }}),{{ end -}}
		),
{{- end }}
	}

	argDefs := []*parameters.ParameterDefinition{
{{- range .Arguments }}
		parameters.NewParameterDefinition(
			"{{ .Name }}",
			{{ .Type | unquoteParameterType}} ,
			parameters.WithHelp("{{ .Help }}"),
			{{ if .Default }}parameters.WithDefault({{ .Default | toGoCode }}),{{ end -}}
		),
{{- end }}
	}

	cmdDescription := cmds.NewCommandDescription("{{ .CommandName }}",
		cmds.WithShort("{{ .ShortDescription }}"),
		cmds.WithLong("{{ .LongDescription }}"),
		cmds.WithFlags(flagDefs...),
		cmds.WithArguments(argDefs...),
	)

	return &{{ .StructName }}{
		CommandDescription: cmdDescription,
		Query:              {{ .QueryConstName }},
		SubQueries:         map[string]string{},
        db: db,
	}, nil
}
`

const runIntoGlazeProcessorTemplate = `func (s *{{.CommandName}}Command) RunIntoGlazeProcessor(
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

	settings := &LsJobsCommandParameters{};
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
`

// smartQuote returns a string with backticks if it contains a newline, or with double quotes otherwise.
func smartQuote(s string) string {
	if strings.Contains(s, "\n") {
		return "`" + s + "`"
	}
	return strconv.Quote(s)
}

const importsTemplate = `
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
`

const constantsTemplate = `{{- range .Constants }}
const {{ .Name }} = {{ .Value | smartQuote }}
{{- end }}
`

// flagTypeToGoType converts a ParameterType to its corresponding Go type as a string.
func flagTypeToGoType(parameterType parameters.ParameterType) string {
	switch parameterType {
	case parameters.ParameterTypeFloat:
		return "float64"
	case parameters.ParameterTypeFloatList:
		return "[]float64"
	case parameters.ParameterTypeInteger:
		return "int"
	case parameters.ParameterTypeIntegerList:
		return "[]int"
	case parameters.ParameterTypeBool:
		return "bool"
	case parameters.ParameterTypeDate:
		return "time.Time"
	case parameters.ParameterTypeStringFromFile,
		parameters.ParameterTypeStringFromFiles,
		parameters.ParameterTypeChoice,
		parameters.ParameterTypeString:
		return "string"
	case parameters.ParameterTypeStringList,
		parameters.ParameterTypeStringListFromFile,
		parameters.ParameterTypeStringListFromFiles,
		parameters.ParameterTypeChoiceList:
		return "[]string"
	case parameters.ParameterTypeFile:
		return "FileData" // Assuming FileData is defined in the GlazedParametersPath
	case parameters.ParameterTypeFileList:
		return "[]FileData" // Assuming FileData is defined in the GlazedParametersPath
	case parameters.ParameterTypeObjectFromFile:
		return "map[string]interface{}"
	case parameters.ParameterTypeObjectListFromFile, parameters.ParameterTypeObjectListFromFiles:
		return "[]map[string]interface{}"
	case parameters.ParameterTypeKeyValue:
		return "map[string]string"
	default:
		return "interface{}"
	}
}

func unquoteParameterType(parameterType parameters.ParameterType) string {
	switch parameterType {
	case parameters.ParameterTypeFloat:
		return "parameters.ParameterTypeFloat"
	case parameters.ParameterTypeFloatList:
		return "parameters.ParameterTypeFloatList"
	case parameters.ParameterTypeInteger:
		return "parameters.ParameterTypeInteger"
	case parameters.ParameterTypeIntegerList:
		return "parameters.ParameterTypeIntegerList"
	case parameters.ParameterTypeBool:
		return "parameters.ParameterTypeBool"
	case parameters.ParameterTypeDate:
		return "parameters.ParameterTypeDate"
	case parameters.ParameterTypeString:
		return "parameters.ParameterTypeString"
	case parameters.ParameterTypeStringList:
		return "parameters.ParameterTypeStringList"
	case parameters.ParameterTypeFile:
		return "parameters.ParameterTypeFile"
	case parameters.ParameterTypeFileList:
		return "parameters.ParameterTypeFileList"
	case parameters.ParameterTypeObjectFromFile:
		return "parameters.ParameterTypeObjectFromFile"
	case parameters.ParameterTypeObjectListFromFile:
		return "parameters.ParameterTypeObjectListFromFile"
	case parameters.ParameterTypeKeyValue:
		return "parameters.ParameterTypeKeyValue"
	default:
		return "unknown"
	}
}

// executeTemplate parses the provided template string and executes it with the given data.
func executeTemplate(templateString string, data interface{}) (string, error) {
	tmpl, err := template.New("template").
		Funcs(template.FuncMap{
			"smartQuote":           smartQuote,
			"flagTypeToGoType":     flagTypeToGoType,
			"unquoteParameterType": unquoteParameterType,
			"toGoCode":             codegen.ToGoCode,
		}).
		Parse(templateString)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, data)
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}

func (gen *SqlCommandCodeGenerator) defineConstants(cmdName string, cmd *cmds.SqlCommand) (string, error) {
	constants := []struct {
		Name  string
		Value string
	}{}

	// Main query constant
	queryConstName := strcase.ToLowerCamel(cmdName) + "CommandQuery"
	constants = append(constants, struct {
		Name  string
		Value string
	}{Name: queryConstName, Value: cmd.Query})

	// SubQuery constants
	for name, subQuery := range cmd.SubQueries {
		subQueryConstName := strcase.ToLowerCamel(cmdName) + "CommandSubQuery" + name
		constants = append(constants, struct {
			Name  string
			Value string
		}{Name: subQueryConstName, Value: subQuery})
	}

	return executeTemplate(constantsTemplate, struct {
		Constants []struct {
			Name  string
			Value string
		}
	}{Constants: constants})
}

func (gen *SqlCommandCodeGenerator) defineStruct(cmdName string) (string, error) {
	data := struct {
		StructName string
	}{
		StructName: strcase.ToCamel(cmdName) + "Command",
	}

	return executeTemplate(structTemplate, data)
}

func (gen *SqlCommandCodeGenerator) defineParametersStruct(cmdName string, cmd *cmds2.CommandDescription) (string, error) {
	p_ := []*parameters.ParameterDefinition{}

	p_ = append(p_, cmd.GetDefaultFlags().ToList()...)
	p_ = append(p_, cmd.GetDefaultArguments().ToList()...)

	data := struct {
		StructName string
		Parameters []*parameters.ParameterDefinition
	}{
		StructName: strcase.ToCamel(cmdName) + "CommandParameters",
		Parameters: p_,
	}

	return executeTemplate(parametersStructTemplate, data)
}

func (gen *SqlCommandCodeGenerator) defineRunIntoGlazedMethod(cmdName string) (string, error) {
	data := struct {
		Receiver         string
		ParametersStruct string
	}{
		Receiver:         strcase.ToCamel(cmdName) + "Command",
		ParametersStruct: strcase.ToCamel(cmdName) + "CommandParameters",
	}

	return executeTemplate(runIntoGlazedTemplate, data)
}

func (gen *SqlCommandCodeGenerator) defineRunIntoGlazeProcessorMethod(cmdName string) (string, error) {
	data := struct {
		CommandName      string
		ParametersStruct string
	}{
		CommandName:      strcase.ToCamel(cmdName),
		ParametersStruct: strcase.ToCamel(cmdName) + "CommandParameters",
	}

	return executeTemplate(runIntoGlazeProcessorTemplate, data)
}

func (gen *SqlCommandCodeGenerator) defineNewFunction(cmdName string, cmd *cmds.SqlCommand) (string, error) {
	description := cmd.Description()
	flags := description.GetDefaultFlags().ToList()
	args := description.GetDefaultArguments().ToList()

	data := struct {
		CommandName      string
		StructName       string
		Flags            []*parameters.ParameterDefinition
		Arguments        []*parameters.ParameterDefinition
		ShortDescription string
		LongDescription  string
		QueryConstName   string
	}{
		CommandName:      strcase.ToCamel(cmdName),
		StructName:       strcase.ToCamel(cmdName) + "Command",
		Flags:            flags,
		Arguments:        args,
		ShortDescription: description.Short,
		LongDescription:  description.Long,
		QueryConstName:   strcase.ToLowerCamel(cmdName) + "CommandQuery",
	}

	return executeTemplate(newFunctionTemplate, data)
}

type SqlCommandCodeGenerator struct {
	PackageName string
}

func (gen *SqlCommandCodeGenerator) GenerateCommandCode(cmd *cmds.SqlCommand) (string, error) {
	cmdName := strcase.ToLowerCamel(cmd.Name)
	var generatedCode strings.Builder

	// Output the package name up front
	generatedCode.WriteString("package " + gen.PackageName + "\n\n")

	generatedCode.WriteString(importsTemplate)

	// Generate constants
	constantsCode, err := gen.defineConstants(cmdName, cmd)
	if err != nil {
		return "", err
	}
	generatedCode.WriteString(constantsCode)

	// Generate struct
	structCode, err := gen.defineStruct(cmdName)
	if err != nil {
		return "", err
	}
	generatedCode.WriteString(structCode)

	// Generate parameters struct
	parametersStructCode, err := gen.defineParametersStruct(cmdName, cmd.Description())
	if err != nil {
		return "", err
	}
	generatedCode.WriteString(parametersStructCode)

	// Generate RunIntoGlazed method
	runIntoGlazedCode, err := gen.defineRunIntoGlazedMethod(cmdName)
	if err != nil {
		return "", err
	}
	generatedCode.WriteString(runIntoGlazedCode)

	// Generate RunIntoGlazed method
	runIntoGlazeProcessorCode, err := gen.defineRunIntoGlazeProcessorMethod(cmdName)
	if err != nil {
		return "", err
	}
	generatedCode.WriteString(runIntoGlazeProcessorCode)

	// Generate New function
	newFunctionCode, err := gen.defineNewFunction(cmdName, cmd)
	if err != nil {
		return "", err
	}
	generatedCode.WriteString(newFunctionCode)

	return generatedCode.String(), nil
}
