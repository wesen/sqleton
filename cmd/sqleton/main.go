package main

import (
	"embed"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/wesen/glazed/pkg/help"
	"github.com/wesen/sqleton/cmd/sqleton/cmds"
	sqleton "github.com/wesen/sqleton/pkg"
	"gopkg.in/natefinch/lumberjack.v2"
	"io"
	"os"
	"strings"
)

var rootCmd = &cobra.Command{
	Use:   "sqleton",
	Short: "sqleton runs SQL queries out of template files",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
	},
}

func initCommands(rootCmd *cobra.Command, helpSystem *help.HelpSystem) ([]*sqleton.SqlCommand, []*sqleton.CommandAlias, error) {
	// Load the variables from the environment
	viper.SetEnvPrefix("sqleton")

	configPath, _ := rootCmd.PersistentFlags().GetString("config")
	if configPath != "" {
		viper.SetConfigFile(configPath)
	} else {
		viper.AddConfigPath(".")
		viper.AddConfigPath("$HOME/.sqleton")
		viper.AddConfigPath("/etc/sqleton")
	}

	// Read the configuration file into Viper
	err := viper.ReadInConfig()
	// if the file does not exist, continue normally
	if _, ok := err.(viper.ConfigFileNotFoundError); ok {
		// Config file not found; ignore error
	} else if err != nil {
		// Config file was found but another error was produced
		return nil, nil, err
	}
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	// Bind the variables to the command-line flags
	err = viper.BindPFlags(rootCmd.PersistentFlags())
	if err != nil {
		return nil, nil, err
	}

	logLevel := viper.GetString("log-level")
	verbose, _ := rootCmd.PersistentFlags().GetBool("verbose")
	if verbose && logLevel != "trace" {
		logLevel = "debug"
	}

	err = InitLogger(&logConfig{
		Level:      logLevel,
		LogFile:    viper.GetString("log-file"),
		LogFormat:  viper.GetString("log-format"),
		WithCaller: viper.GetBool("with-caller"),
	})
	cobra.CheckErr(err)

	log.Debug().
		Str("config", viper.ConfigFileUsed()).
		Msg("Loaded configuration")

	commands, aliases, err := sqleton.LoadSqlCommandsFromEmbedFS(queriesFS, ".", "queries/")
	if err != nil {
		return nil, nil, err
	}
	err = helpSystem.LoadSectionsFromEmbedFS(queriesFS, "queries/doc")
	if err != nil {
		return nil, nil, err
	}

	repositoryCommands, repositoryAliases, err := loadRepositoryCommands(helpSystem)
	if err != nil {
		return nil, nil, err
	}

	commands = append(commands, repositoryCommands...)
	aliases = append(aliases, repositoryAliases...)

	err = sqleton.AddCommandsToRootCommand(rootCmd, commands, aliases)
	if err != nil {
		return nil, nil, err
	}

	return commands, aliases, nil
}

func loadRepositoryCommands(helpSystem *help.HelpSystem) ([]*sqleton.SqlCommand, []*sqleton.CommandAlias, error) {
	repositories := viper.GetStringSlice("repositories")

	defaultDirectory := "$HOME/.sqleton/queries"
	repositories = append(repositories, defaultDirectory)

	commands := make([]*sqleton.SqlCommand, 0)
	aliases := make([]*sqleton.CommandAlias, 0)

	for _, repository := range repositories {
		repository = os.ExpandEnv(repository)

		// check that repository exists and is a directory
		s, err := os.Stat(repository)

		if os.IsNotExist(err) {
			log.Debug().Msgf("Repository %s does not exist", repository)
			continue
		} else if err != nil {
			log.Warn().Msgf("Error while checking directory %s: %s", repository, err)
			continue
		}

		if s == nil || !s.IsDir() {
			log.Warn().Msgf("Repository %s is not a directory", repository)
		} else {
			docDir := fmt.Sprintf("%s/doc", repository)
			commands_, aliases_, err := sqleton.LoadSqlCommandsFromDirectory(repository, repository)
			if err != nil {
				return nil, nil, err
			}
			commands = append(commands, commands_...)
			aliases = append(aliases, aliases_...)

			_, err = os.Stat(docDir)
			if os.IsNotExist(err) {
				continue
			} else if err != nil {
				log.Debug().Err(err).Msgf("Error while checking directory %s", docDir)
				continue
			}
			err = helpSystem.LoadSectionsFromDirectory(docDir)
			if err != nil {
				log.Warn().Err(err).Msgf("Error while loading help sections from directory %s", repository)
				continue
			}
		}
	}
	return commands, aliases, nil
}

type logConfig struct {
	WithCaller bool
	Level      string
	LogFormat  string
	LogFile    string
}

func InitLogger(config *logConfig) error {
	if config.WithCaller {
		log.Logger = log.With().Caller().Logger()
	}
	// default is json
	var logWriter io.Writer
	if config.LogFormat == "text" {
		logWriter = zerolog.ConsoleWriter{Out: os.Stderr}
	} else {
		logWriter = os.Stderr
	}

	if config.LogFile != "" {
		logWriter = io.MultiWriter(
			logWriter,
			zerolog.ConsoleWriter{
				NoColor: true,
				Out: &lumberjack.Logger{
					Filename:   config.LogFile,
					MaxSize:    10, // megabytes
					MaxBackups: 3,
					MaxAge:     28,    //days
					Compress:   false, // disabled by default
				},
			})
	}

	log.Logger = log.Output(logWriter)

	switch config.Level {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	case "fatal":
		zerolog.SetGlobalLevel(zerolog.FatalLevel)
	}

	return nil
}

func main() {
	_ = rootCmd.Execute()
}

//go:embed doc/*
var docFS embed.FS

//go:embed queries/*
var queriesFS embed.FS

func init() {
	helpSystem := help.NewHelpSystem()
	err := helpSystem.LoadSectionsFromEmbedFS(docFS, ".")
	if err != nil {
		panic(err)
	}

	helpFunc, usageFunc := help.GetCobraHelpUsageFuncs(helpSystem)
	helpTemplate, usageTemplate := help.GetCobraHelpUsageTemplates(helpSystem)

	_ = usageFunc
	_ = usageTemplate

	rootCmd.SetHelpFunc(helpFunc)
	rootCmd.SetUsageFunc(usageFunc)
	rootCmd.SetHelpTemplate(helpTemplate)
	rootCmd.SetUsageTemplate(usageTemplate)

	helpCmd := help.NewCobraHelpCommand(helpSystem)
	rootCmd.SetHelpCommand(helpCmd)

	// db connection persistent base flags
	rootCmd.PersistentFlags().Bool("use-dbt-profiles", false, "Use dbt profiles.yml to connect to databases")
	rootCmd.PersistentFlags().String("dbt-profiles-path", "", "Path to dbt profiles.yml (default: ~/.dbt/profiles.yml)")
	rootCmd.PersistentFlags().String("dbt-profile", "default", "Name of dbt profile to use (default: default)")

	// logging flags
	rootCmd.PersistentFlags().Bool("with-caller", false, "Log caller")
	rootCmd.PersistentFlags().String("log-level", "info", "Log level (debug, info, warn, error, fatal)")
	rootCmd.PersistentFlags().String("log-format", "text", "Log format (json, text)")
	rootCmd.PersistentFlags().String("log-file", "", "Log file (default: stderr)")

	// more normal flags
	rootCmd.PersistentFlags().StringP("host", "H", "", "Database host")
	rootCmd.PersistentFlags().StringP("database", "D", "", "Database name")
	rootCmd.PersistentFlags().StringP("user", "u", "", "Database user")
	rootCmd.PersistentFlags().StringP("password", "p", "", "Database password")
	rootCmd.PersistentFlags().IntP("port", "P", 3306, "Database port")
	rootCmd.PersistentFlags().StringP("schema", "s", "", "Database schema (when applicable)")
	rootCmd.PersistentFlags().StringP("type", "t", "mysql", "Database type (mysql, postgres, etc.)")

	rootCmd.PersistentFlags().String("repository", "", "Directory with additional commands to load (default ~/.sqleton/queries)")

	// dsn and driver
	rootCmd.PersistentFlags().String("dsn", "", "Database DSN")
	rootCmd.PersistentFlags().String("driver", "", "Database driver")

	rootCmd.PersistentFlags().String("config", "", "Path to config file (default ~/.sqleton/config.yml)")
	rootCmd.PersistentFlags().Bool("verbose", false, "Verbose output")

	rootCmd.AddCommand(cmds.DbCmd)
	rootCmd.AddCommand(cmds.RunCmd)
	rootCmd.AddCommand(cmds.QueryCmd)
	rootCmd.AddCommand(cmds.SelectCmd)
	rootCmd.AddCommand(cmds.MysqlCmd)

	cmds.InitializeMysqlCmd(queriesFS, helpSystem)

	// parse the flags one time just to catch --config
	err = rootCmd.ParseFlags(os.Args[1:])
	if err != nil && err != pflag.ErrHelp {
		cobra.CheckErr(err)
	}

	commands, aliases, err := initCommands(rootCmd, helpSystem)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error initializing commands: %s\n", err)
		os.Exit(1)
	}

	queriesCmd := cmds.AddQueriesCmd(commands, aliases)
	rootCmd.AddCommand(queriesCmd)
}
