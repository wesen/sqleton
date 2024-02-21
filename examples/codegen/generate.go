package codegen

//go:generate go run ../../cmd/sqleton codegen --package-name codegen  --output-dir . ls-jobs.yaml
//go:generate go run ../../cmd/sqleton codegen --use-template-codegen --package-name templates  --output-dir templates/ ls-jobs.yaml
