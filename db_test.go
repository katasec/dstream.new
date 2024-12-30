package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	_ "github.com/denisenkom/go-mssqldb" // SQL Server driver
)

func TestInsertRandomCars(t *testing.T) {
	// Set up the number of random records to insert
	N := 10 // Change this value to insert a different number of records

	// Get the connection string from the environment variable
	connString := os.Getenv("DSTREAM_DB_CONNECTION_STRING")
	if connString == "" {
		t.Fatal("DSTREAM_DB_CONNECTION_STRING is not set")
	}

	// Connect to the database
	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		t.Fatalf("Failed to connect to the database: %v", err)
	}
	defer db.Close()

	// Ensure the connection is valid
	err = db.Ping()
	if err != nil {
		t.Fatalf("Failed to ping the database: %v", err)
	}

	// Clear the Cars table
	_, err = db.Exec("DELETE FROM [dbo].[Cars]")
	if err != nil {
		t.Fatalf("Failed to clear the Cars table: %v", err)
	}

	rand.Seed(time.Now().UnixNano())
	brandNames := []string{"Toyota", "Honda", "Ford", "BMW", "Audi"}
	colors := []string{"Red", "Blue", "Green", "Black", "White"}

	// Insert random records into the Cars table
	for i := 0; i < N; i++ {
		brand := brandNames[rand.Intn(len(brandNames))]
		color := colors[rand.Intn(len(colors))]

		_, err = db.Exec("INSERT INTO [dbo].[Cars] (BrandName, Color) VALUES (@p1, @p2)", brand, color)
		if err != nil {
			t.Fatalf("Failed to insert record %d: %v", i+1, err)
		}
	}

	fmt.Printf("Successfully inserted %d random records into the Cars table.\n", N)
}

func TestInsertRandomPersons(t *testing.T) {
	// Set up the number of random records to insert
	N := 10 // Change this value to insert a different number of records

	// Get the connection string from the environment variable
	connString := os.Getenv("DSTREAM_DB_CONNECTION_STRING")
	if connString == "" {
		t.Fatal("DSTREAM_DB_CONNECTION_STRING is not set")
	}

	// Connect to the database
	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		t.Fatalf("Failed to connect to the database: %v", err)
	}
	defer db.Close()

	// Ensure the connection is valid
	err = db.Ping()
	if err != nil {
		t.Fatalf("Failed to ping the database: %v", err)
	}

	// Clear the Persons table
	_, err = db.Exec("DELETE FROM [dbo].[Persons]")
	if err != nil {
		t.Fatalf("Failed to clear the Persons table: %v", err)
	}

	rand.Seed(time.Now().UnixNano())
	firstNames := []string{"John", "Jane", "Alice", "Bob", "Charlie"}
	lastNames := []string{"Smith", "Doe", "Brown", "Wilson", "Taylor"}

	// Insert random records into the Persons table
	for i := 0; i < N; i++ {
		firstName := firstNames[rand.Intn(len(firstNames))]
		lastName := lastNames[rand.Intn(len(lastNames))]

		_, err = db.Exec("INSERT INTO [dbo].[Persons] (FirstName, LastName) VALUES (@p1, @p2)", firstName, lastName)
		if err != nil {
			t.Fatalf("Failed to insert record %d: %v", i+1, err)
		}
	}

	fmt.Printf("Successfully inserted %d random records into the Persons table.\n", N)
}
