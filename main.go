package main

import (
	"fmt"
	"os"

	"github.com/containerd/log"
	"github.com/docker/go-plugins-helpers/sdk"
)

func main() {
	if lvl := os.Getenv("LOG_LEVEL"); lvl != "" {
		if err := log.SetLevel(lvl); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "invalid log level: ", err)
			os.Exit(1)
		}
	}

	h := sdk.NewHandler(`{"Implements": ["LogDriver"]}`)
	handlers(&h, newDriver())
	if err := h.ServeUnix("jsonfile", 0); err != nil {
		panic(err)
	}
}
