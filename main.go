package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	if err := execute(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, "metric-ferry:", err)
		os.Exit(1)
	}
}

func execute(args []string) error {
	c, err := parseConfig(args, os.Stderr)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger := log.New(os.Stderr, "", log.LstdFlags)
	if c.dryRun {
		logger.Print("dry run: encoding requests without network writes")
	}
	_, err = run(ctx, c, logger)
	return err
}
