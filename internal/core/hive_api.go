package core

import "github.com/guywaldman/golars/internal/hive"

func validatePartitionNames(names []string) error {
	return hive.ValidatePartitionNames(names)
}
