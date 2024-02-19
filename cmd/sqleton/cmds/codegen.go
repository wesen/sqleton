package cmds

import (
	"fmt"
	"github.com/go-go-golems/glazed/pkg/cmds"
	"github.com/go-go-golems/glazed/pkg/cmds/alias"
	"github.com/go-go-golems/glazed/pkg/cmds/loaders"
	cmds2 "github.com/go-go-golems/sqleton/pkg/cmds"
	"github.com/go-go-golems/sqleton/pkg/codegen"
	"github.com/go-go-golems/sqleton/pkg/codegen/templates"
	"github.com/spf13/cobra"
	"os"
	"path"
	"strings"
)

func NewCodegenCommand() *cobra.Command {
	ret := &cobra.Command{
		Use:   "codegen [file...]",
		Short: "A program to convert Sqleton YAML commands into Go code",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			packageName := cmd.Flag("package-name").Value.String()
			outputDir := cmd.Flag("output-dir").Value.String()
			outputFile := cmd.Flag("output-file").Value.String()
			useTemplateCodegen, _ := cmd.PersistentFlags().GetBool("use-template-codegen")

			if outputFile != "" && len(args) > 1 {
				return fmt.Errorf("output-file can only be used with a single input file")
			}

			for _, fileName := range args {
				loader := &cmds2.SqlCommandLoader{
					DBConnectionFactory: nil,
				}

				fs_, fileName, err := loaders.FileNameToFsFilePath(fileName)
				if err != nil {
					return err
				}
				cmds_, err := loader.LoadCommands(fs_, fileName, []cmds.CommandDescriptionOption{}, []alias.Option{})
				if err != nil {
					return err
				}
				if len(cmds_) != 1 {
					return fmt.Errorf("expected exactly one command, got %d", len(cmds_))
				}
				cmd := cmds_[0].(*cmds2.SqlCommand)

				var s string
				if useTemplateCodegen {
					generator := &templates.SqlCommandCodeGenerator{
						PackageName: packageName,
					}
					f, err := generator.GenerateCommandCode(cmd)
					if err != nil {
						return err
					}

					s = f
				} else {
					generator := &codegen.SqlCommandCodeGenerator{
						PackageName: packageName,
					}
					f, err := generator.GenerateCommandCode(cmd)
					if err != nil {
						return err
					}

					s = f.GoString()
				}

				// store in path.go after removing .yaml
				p, _ := strings.CutSuffix(path.Base(fileName), ".yaml")
				p = p + ".go"
				if outputFile != "" {
					p = outputFile
				}
				p = path.Join(outputDir, p)

				// ensure directory for p exists
				err = os.MkdirAll(path.Dir(p), 0755)
				cobra.CheckErr(err)

				fmt.Printf("Converting %s to %s\n", fileName, p)
				err = os.WriteFile(p, []byte(s), 0644)
				cobra.CheckErr(err)
			}

			return nil
		},
	}

	ret.PersistentFlags().StringP("output-dir", "o", ".", "Output directory for generated code")
	ret.PersistentFlags().StringP("output-file", "O", "", "Output file for generated code")
	ret.PersistentFlags().StringP("package-name", "p", "main", "Package name for generated code")
	ret.PersistentFlags().Bool("use-template-codegen", false, "Use the template codegen")
	return ret
}
