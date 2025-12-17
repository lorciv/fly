package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

var sourcedir = flag.String("sourcedir", "migrations", "directory that contains database migration files")

func main() {
	log.SetFlags(0)

	flag.Parse()

	if flag.NArg() < 1 {
		log.Fatal("missing cmd")
	}

	cmd := flag.Arg(0)
	switch cmd {
	case "new":
		last := "0000_unnamed.up.sql"
		entries, err := os.ReadDir(*sourcedir)
		if err != nil {
			log.Fatal(err)
		}
		if len(entries) > 0 {
			last = entries[len(entries)-1].Name()
		}

		serial, _, found := strings.Cut(last, "_")
		if !found {
			log.Fatal("invalid filename: missing counter")
		}
		n, err := strconv.Atoi(serial)
		if err != nil {
			log.Fatalf("invalid filename: %s", err)
		}

		nextSerial := fmt.Sprintf("%04d", n+1)

		label := flag.Arg(1)
		if label == "" {
			label = "unnamed"
		}
		label = strings.ReplaceAll(label, " ", "_")

		for _, t := range []string{"up", "down"} {
			filename := fmt.Sprintf("%s/%s_%s.%s.sql", *sourcedir, nextSerial, label, t)
			if _, err := os.Create(filename); err != nil {
				log.Fatal(err)
			}
		}
	default:
		log.Fatal("unknown cmd")
	}
}
