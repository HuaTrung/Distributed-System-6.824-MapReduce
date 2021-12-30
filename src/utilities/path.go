package uti

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

func GetPath() string {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		fmt.Println(ok)
		os.Exit(1)
	}
	dirname := filepath.Dir(filename)
	return dirname[:len(dirname)-len("utilities")]
}
