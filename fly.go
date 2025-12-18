package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	_ "github.com/lib/pq"
)

func initialize(db *sql.DB) error {
	_, err := db.Exec("CREATE TABLE IF NOT EXISTS migration (id VARCHAR(256) PRIMARY KEY, applied TIMESTAMP DEFAULT current_timestamp)")
	if err != nil {
		return fmt.Errorf("could not create migration table: %v", err)
	}
	return nil
}

type migration struct {
	id      string
	applied time.Time
}

// listRun reads all migrations that have been executed on the database.
func listRun(db *sql.DB) ([]migration, error) {
	rows, err := db.Query("SELECT id, applied FROM migration ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []migration
	for rows.Next() {
		var r migration
		if err := rows.Scan(&r.id, &r.applied); err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func hasRun(db *sql.DB, migration string) (bool, error) {
	var found int
	err := db.QueryRow("SELECT 1 FROM migration WHERE id = $1", migration).Scan(&found)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("could not check migration %s: %v", migration, err)
	}
	return true, nil
}

// listDir reads all migrations from the directory "migrations" sorted by increasing ID.
func listDir() ([]string, error) {
	entries, err := os.ReadDir("migrations")
	if err != nil {
		return nil, err
	}

	var migrations []string
	for _, e := range entries {
		id, found := strings.CutSuffix(e.Name(), ".up.sql")
		if found {
			migrations = append(migrations, id)
		}
	}

	sort.Strings(migrations)

	return migrations, nil
}

func run(tx *sql.Tx, filename string) error {
	script, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(string(script)); err != nil {
		return fmt.Errorf("could not run %s: %s", filename, err)
	}
	return nil
}

func register(tx *sql.Tx, migration string) error {
	_, err := tx.Exec("INSERT INTO migration VALUES ($1)", migration)
	if err != nil {
		return fmt.Errorf("could not create migration: %v", err)
	}
	return nil
}

func unregister(tx *sql.Tx, migration string) error {
	_, err := tx.Exec("DELETE FROM migration WHERE id = $1", migration)
	if err != nil {
		return fmt.Errorf("could not delete migration: %v", err)
	}
	return nil
}

var sourcedir = flag.String("sourcedir", "migrations", "directory that contains database migration files")

func main() {
	log.SetFlags(0)
	log.SetPrefix("fly: ")

	flag.Parse()

	if flag.NArg() < 1 {
		log.Fatal("missing cmd")
	}

	cmd := flag.Arg(0)
	switch cmd {
	case "init":
		db, err := sql.Open("postgres", "")
		if err != nil {
			log.Fatal(err)
		}
		if err := db.Ping(); err != nil {
			log.Fatal(err)
		}

		if err := initialize(db); err != nil {
			log.Fatal(err)
		}
	case "status":
		db, err := sql.Open("postgres", "")
		if err != nil {
			log.Fatal(err)
		}
		if err := db.Ping(); err != nil {
			log.Fatal(err)
		}

		migrations, err := listRun(db)
		if err != nil {
			log.Fatal(err)
		}
		writer := tabwriter.NewWriter(os.Stdout, 1, 3, 1, ' ', 0)
		format := "%s\t%s\n"
		fmt.Fprintf(writer, format, "ID", "APPLIED")
		fmt.Fprintf(writer, format, "--", "-------")
		for _, m := range migrations {
			fmt.Fprintf(writer, format, m.id, m.applied.Format(time.DateTime))
		}
		writer.Flush()
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
	case "up":
		db, err := sql.Open("postgres", "")
		if err != nil {
			log.Fatal(err)
		}
		tx, err := db.Begin()
		if err != nil {
			log.Fatal(err)
		}
		defer tx.Rollback()

		migrations, err := listDir()
		if err != nil {
			log.Fatal(err)
		}
		for _, id := range migrations {
			ok, err := hasRun(db, id)
			if err != nil {
				log.Fatal(err)
			}
			if ok {
				continue
			}
			fmt.Println(id)
			if err := run(tx, *sourcedir+"/"+id+".up.sql"); err != nil {
				log.Fatal(err)
			}
			if err := register(tx, id); err != nil {
				log.Fatal(err)
			}
		}

		if err := tx.Commit(); err != nil {
			log.Fatal(err)
		}
	case "down":
		db, err := sql.Open("postgres", "")
		if err != nil {
			log.Fatal(err)
		}
		tx, err := db.Begin()
		if err != nil {
			log.Fatal(err)
		}
		defer tx.Rollback()

		n := 1
		if arg := flag.Arg(1); arg != "" {
			var err error
			n, err = strconv.Atoi(arg)
			if err != nil {
				log.Fatal(err)
			}
		}
		_ = n

		migrations, err := listRun(db)
		if err != nil {
			log.Fatal(err)
		}
		for i := 0; i < n; i++ {
			j := len(migrations) - 1 - i
			if j < 0 {
				break
			}
			id := migrations[j].id
			filename := fmt.Sprintf("%s/%s.down.sql", *sourcedir, id)
			if err := run(tx, filename); err != nil {
				log.Fatal(err)
			}
			if err := unregister(tx, id); err != nil {
				log.Fatal(err)
			}
			fmt.Println(id)
		}

		if err := tx.Commit(); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatal("unknown cmd")
	}
}
