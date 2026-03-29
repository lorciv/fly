package main

import (
	"database/sql"
	"errors"
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

// initMigrationTable ensures that the migration table on the database is present.
func initMigrationTable(db *sql.DB) error {
	_, err := db.Exec("CREATE TABLE IF NOT EXISTS migration (id VARCHAR(256) PRIMARY KEY, applied TIMESTAMP DEFAULT current_timestamp)")
	if err != nil {
		return fmt.Errorf("could not create migration table: %v", err)
	}
	return nil
}

// migration represents a migration applied to the database.
type migration struct {
	id      string
	applied time.Time
}

// listAppliedMigrations reads all migrations that have been executed on the database.
func listAppliedMigrations(db *sql.DB) ([]migration, error) {
	rows, err := db.Query("SELECT id, applied FROM migration ORDER BY applied, id")
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

// isMigrationApplied checks if the migration has run on the database.
func isMigrationApplied(db *sql.DB, migration string) (bool, error) {
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

// listDirMigrations reads all migrations from the configured directory, sorted by increasing ID.
func listDirMigrations() ([]string, error) {
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

// runScript executes the SQL script on the database.
func runScript(tx *sql.Tx, filename string) error {
	script, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(string(script)); err != nil {
		return fmt.Errorf("could not run %s: %s", filename, err)
	}
	return nil
}

// registerMigration inserts a new row for the given migration into the migration table.
func registerMigration(tx *sql.Tx, migration string) error {
	_, err := tx.Exec("INSERT INTO migration VALUES ($1)", migration)
	if err != nil {
		return fmt.Errorf("could not create migration: %v", err)
	}
	return nil
}

// unregisterMigration deletes the row for the given migration from the migration table.
func unregisterMigration(tx *sql.Tx, migration string) error {
	_, err := tx.Exec("DELETE FROM migration WHERE id = $1", migration)
	if err != nil {
		return fmt.Errorf("could not delete migration: %v", err)
	}
	return nil
}

var sourcedir = flag.String("sourcedir", "migrations", "directory that contains database migration files")

func doInit() error {
	db, err := sql.Open("postgres", "")
	if err != nil {
		return err
	}
	if err := initMigrationTable(db); err != nil {
		return err
	}
	return nil
}

func doStatus() error {
	db, err := sql.Open("postgres", "")
	if err != nil {
		return err
	}

	migrations, err := listAppliedMigrations(db)
	if err != nil {
		return err
	}

	writer := tabwriter.NewWriter(os.Stdout, 1, 3, 1, ' ', 0)
	format := "%s\t%s\n"
	fmt.Fprintf(writer, format, "ID", "APPLIED")
	fmt.Fprintf(writer, format, "--", "-------")
	for _, m := range migrations {
		fmt.Fprintf(writer, format, m.id, m.applied.Format(time.DateTime))
	}
	writer.Flush()

	return nil
}

func doNew() error {
	last := "0000_unnamed.up.sql"
	entries, err := os.ReadDir(*sourcedir)
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		last = entries[len(entries)-1].Name()
	}

	serial, _, found := strings.Cut(last, "_")
	if !found {
		return errors.New("invalid filename: missing counter")
	}
	n, err := strconv.Atoi(serial)
	if err != nil {
		return fmt.Errorf("invalid filename: %s", err)
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
			return err
		}
	}

	return nil
}

func doUp() error {
	db, err := sql.Open("postgres", "")
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	migrations, err := listDirMigrations()
	if err != nil {
		return err
	}
	for _, id := range migrations {
		ok, err := isMigrationApplied(db, id)
		if err != nil {
			return err
		}
		if ok {
			continue
		}
		if err := runScript(tx, *sourcedir+"/"+id+".up.sql"); err != nil {
			return err
		}
		if err := registerMigration(tx, id); err != nil {
			return err
		}
		fmt.Println("up", id)
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func doDown() error {
	db, err := sql.Open("postgres", "")
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	n := 1
	if arg := flag.Arg(1); arg != "" {
		var err error
		n, err = strconv.Atoi(arg)
		if err != nil {
			return err
		}
	}
	_ = n

	migrations, err := listAppliedMigrations(db)
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		j := len(migrations) - 1 - i
		if j < 0 {
			break
		}
		id := migrations[j].id
		filename := fmt.Sprintf("%s/%s.down.sql", *sourcedir, id)
		if err := runScript(tx, filename); err != nil {
			return err
		}
		if err := unregisterMigration(tx, id); err != nil {
			return err
		}
		fmt.Println("down", id)
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("fly: ")

	flag.Parse()

	if flag.NArg() < 1 {
		log.Fatal("usage: fly <command>")
	}

	var (
		cmd = flag.Arg(0)
		err error
	)
	switch cmd {
	case "init":
		err = doInit()
	case "status":
		err = doStatus()
	case "new":
		err = doNew()
	case "up":
		err = doUp()
	case "down":
		err = doDown()
	default:
		err = errors.New("unknown cmd")
	}
	if err != nil {
		log.Fatal(err)
	}
}
