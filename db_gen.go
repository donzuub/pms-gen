package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"cloud.google.com/go/firestore"
	_ "github.com/mattn/go-sqlite3"
	"github.com/spf13/viper"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ErrorDetail struct {
	Message string      `json:"message"`
	Name    string      `json:"name"`
	Stack   string      `json:"stack"`
	Cause   interface{} `json:"cause,omitempty"`
}

func (e *ErrorDetail) UnmarshalJSON(data []byte) error {
	type Alias ErrorDetail
	aux := &struct {
		Cause interface{} `json:"cause,omitempty"`
		*Alias
	}{
		Alias: (*Alias)(e),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	switch aux.Cause.(type) {
	case map[string]interface{}:
		e.Cause = aux.Cause
	case string:
		e.Cause = map[string]interface{}{
			"message": aux.Cause,
		}
	default:
		e.Cause = nil
	}

	return nil
}

type BotRequest struct {
	BotId         string                 `json:"botId"`
	ClientStatus  string                 `json:"clientStatus"`
	CorrelationId string                 `json:"correlationId"`
	CreateTime    time.Time              `json:"createTime"`
	Errors        []ErrorDetail          `json:"errors"`
	EventId       string                 `json:"eventId"`
	EventTime     string                 `json:"eventTime"`
	Message       map[string]interface{} `json:"message"`
	MessageIds    []string               `json:"messageIds"`
	OfficeId      string                 `json:"officeId"`
	PatientId     string                 `json:"patientId"`
	Status        string                 `json:"status"`
	Username      sql.NullString         `json:"username,omitempty"`
	Result        map[string]interface{} `json:"result"`
}

var (
	acknowledgedRecords = sync.Map{}
	debugMode           bool
)

func main() {

	flag.BoolVar(&debugMode, "debug", false, "Enable debug mode to log raw Firestore documents")
	flag.Parse()

	// Load configuration from .zuubrc file
	viper.SetConfigName(".zuubrc")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("$HOME")

	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("❌ Error reading config file: %v", err)
	}

	environment := viper.GetString("environment")
	if environment == "" {
		log.Fatalf("❌ Environment key not found in .zuubrc file")
	}

	intervalMs := viper.GetInt("interval_ms")
	if intervalMs == 0 {
		log.Fatalf("❌ interval_ms key not found in .zuubrc file")
	}

	// Define SQLite database file path
	connStr := "data.db"

	// Get Firestore credentials and project ID from environment variables
	var credsFile, projectID string
	if environment == "STAGE" {
		credsFile = os.Getenv("GCP_STAGE_CREDS")
		projectID = "dev-zuub"
		log.Printf("🔧 Using STAGE environment with project ID: %s", projectID)
	} else if environment == "PROD" {
		credsFile = os.Getenv("GCP_PROD_CREDS")
		projectID = "zuubapp"
		log.Printf("🚀 Using PROD environment with project ID: %s", projectID)
	} else {
		log.Fatalf("❌ Invalid environment: %s", environment)
	}

	// Initialize Firestore client
	ctx := context.Background()
	client, err := firestore.NewClient(ctx, projectID, option.WithCredentialsFile(credsFile))
	if err != nil {
		log.Fatalf("❌ Failed to create Firestore client: %v", err)
	}
	defer client.Close()
	log.Println("📦 Firestore client initialized")

	// Initialize SQLite connection
	db, err := connectToSQLite(connStr)
	if err != nil {
		log.Fatalf("❌ Failed to connect to SQLite: %v", err)
	}
	defer db.Close()
	log.Println("🔗 SQLite connection established")

	// Create the table if it doesn't exist
	createTable(db)

	// Create a channel to process BotRequest documents
	docChan := make(chan *firestore.DocumentSnapshot, 100)

	// Use a wait group to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Number of worker goroutines
	numWorkers := runtime.NumCPU()

	// Start worker goroutines
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(ctx, &wg, db, docChan)
	}

	// Define the timestamp from the current time
	startTime := time.Now()

	// Watch for new changes in Firestore
	log.Println("👀 Watching for new changes in Firestore")
	ticker := time.NewTicker(time.Duration(intervalMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			checkForNewChanges(ctx, client, db, docChan, startTime)
		}
	}

	// Wait for the workers to finish processing the initial records
	wg.Wait()
}

func connectToSQLite(connStr string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", connStr)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func createTable(db *sql.DB) {
	query := `
	CREATE TABLE IF NOT EXISTS main (
		botid TEXT,
		correlationid TEXT,
		createtime TEXT,
		officeid TEXT,
		patientid TEXT,
		status TEXT,
		result TEXT,
		docid TEXT PRIMARY KEY,
		error TEXT,
		stack TEXT
	);
	CREATE TABLE IF NOT EXISTS acknowledged_documents (
		docid TEXT PRIMARY KEY,
		botid TEXT,
		correlationid TEXT,
		createtime TEXT,
		officeid TEXT,
		patientid TEXT,
		status TEXT,
		result TEXT,
		error TEXT,
		stack TEXT
	);
	`
	_, err := db.Exec(query)
	if err != nil {
		log.Fatalf("❌ Failed to create table: %v", err)
	}
}

func worker(ctx context.Context, wg *sync.WaitGroup, db *sql.DB, docChan <-chan *firestore.DocumentSnapshot) {
	defer wg.Done()

	for doc := range docChan {
		var botRequest BotRequest
		docID := doc.Ref.ID

		data := doc.Data()

		if debugMode {
			rawDoc, err := json.MarshalIndent(data, "", "  ")
			if err != nil {
				log.Printf("❌ Failed to marshal Firestore document data: %v", err)
			} else {
				log.Printf("📄 Raw Firestore document (ID: %s): %s", docID, string(rawDoc))
			}
		}

		createTime, err := extractTimestamp(data, "createTime")
		if err != nil {
			log.Printf("❌ Failed to extract createTime: %v", err)
			continue
		}
		botRequest.CreateTime = createTime

		marshaledData, err := json.Marshal(data)
		if err != nil {
			log.Printf("❌ Failed to marshal Firestore document data: %v", err)
			continue
		}

		if err := json.Unmarshal(marshaledData, &botRequest); err != nil {
			log.Printf("❌ Failed to decode Firestore document: %v", err)
			continue
		}

		errorName, errorStack := extractErrors(ctx, doc.Ref)

		botRequest.Errors = nil

		if botRequest.MessageIds == nil {
			botRequest.MessageIds = []string{}
		}

		if nestedData, ok := data["data"].(map[string]interface{}); ok {
			if patientID, exists := nestedData["patientId"].(string); exists {
				botRequest.PatientId = patientID
			}
			if officeInfo, exists := nestedData["offices"].(map[string]interface{}); exists {
				if appointmentOffice, exists := officeInfo["appointment"].(map[string]interface{}); exists {
					if officeID, exists := appointmentOffice["id"].(string); exists {
						botRequest.OfficeId = officeID
					}
				}
			}
			if botRequest.OfficeId == "" {
				if creationOfficeID, exists := nestedData["creationOfficeId"].(string); exists {
					botRequest.OfficeId = creationOfficeID
				}
			}
			botRequest.Result = nestedData
		}

		if botRequest.ClientStatus == "Acknowledged" {
			if err := trackAcknowledgedDocument(db, botRequest, docID, errorName, errorStack); err != nil {
				log.Printf("❌ Failed to track acknowledged document: %v", err)
			} else {
				log.Printf("🔄 Tracking acknowledged document: Document ID: %s", docID)
			}
			continue
		}

		if botRequest.ClientStatus == "Succeeded" || botRequest.ClientStatus == "Failed" {
			if botRequest.OfficeId == "" {
				if creationOfficeID, exists := botRequest.Result["creationOfficeId"].(string); exists {
					botRequest.OfficeId = creationOfficeID
				}
			}

			log.Printf("🔍 Processing document: Document ID %s, CreateTime %v", docID, botRequest.CreateTime)
			insertIntoSQLite(db, botRequest, docID, errorName, errorStack)
			deleteAcknowledgedDocument(db, botRequest.CorrelationId)
		}
	}
}

func extractTimestamp(data map[string]interface{}, field string) (time.Time, error) {
	value, ok := data[field]
	if !ok || value == nil {
		return time.Time{}, status.Errorf(codes.InvalidArgument, "expected Firestore timestamp or time.Time, got <nil>")
	}
	switch v := value.(type) {
	case string:
		parsedTime, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return time.Time{}, status.Errorf(codes.InvalidArgument, "expected Firestore timestamp or time.Time, got invalid string format")
		}
		return parsedTime, nil
	case map[string]interface{}:
		seconds, secondsOk := v["seconds"].(float64)
		nanos, nanosOk := v["nanos"].(float64)
		if !secondsOk || !nanosOk {
			return time.Time{}, status.Errorf(codes.InvalidArgument, "expected Firestore timestamp with seconds and nanos fields")
		}
		timestamp := time.Unix(int64(seconds), int64(nanos))
		return timestamp, nil
	case time.Time:
		return v, nil
	default:
		return time.Time{}, status.Errorf(codes.InvalidArgument, "expected Firestore timestamp or time.Time, got %T", v)
	}
}

func checkForNewChanges(ctx context.Context, client *firestore.Client, db *sql.DB, docChan chan<- *firestore.DocumentSnapshot, startTime time.Time) {
	retryCount := 0
	maxRetries := 5

	for {
		iter := client.Collection("BotRequests").Where("createTime", ">", startTime).Snapshots(ctx)
		defer iter.Stop()

		for {
			docSnap, err := iter.Next()
			if err != nil {
				if status.Code(err) == codes.NotFound {
					time.Sleep(5 * time.Second)
					continue
				}
				if status.Code(err) == codes.DeadlineExceeded || status.Code(err) == codes.Unavailable {
					log.Printf("❌ Error watching Firestore snapshots: %v. Retrying...", err)
					if retryCount < maxRetries {
						retryCount++
						time.Sleep(time.Duration(retryCount) * time.Second)
						break
					} else {
						log.Fatalf("❌ Error watching Firestore snapshots: %v. Maximum retry attempts reached.", err)
					}
				}
				log.Printf("❌ Error watching Firestore snapshots: %v", err)
				return
			}

			for _, docChange := range docSnap.Changes {
				if docChange.Kind == firestore.DocumentAdded || docChange.Kind == firestore.DocumentModified {
					docChan <- docChange.Doc
				}
			}
		}
		retryCount = 0
	}
}

func insertIntoSQLite(db *sql.DB, botRequest BotRequest, docID string, errorName string, errorStack string) {
	resultJSON, err := json.Marshal(botRequest.Result)
	if err != nil {
		log.Printf("❌ Failed to marshal result field: %v", err)
		return
	}

	query := `
        INSERT INTO main (botid, correlationid, createtime, officeid, patientid, status, result, docid, error, stack)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(docid) DO UPDATE SET
            botid = excluded.botid,
            correlationid = excluded.correlationid,
            createtime = excluded.createtime,
            officeid = excluded.officeid,
            patientid = excluded.patientid,
            status = excluded.status,
            result = excluded.result,
            error = excluded.error,
            stack = excluded.stack;
    `
	_, err = db.Exec(query,
		botRequest.BotId, botRequest.CorrelationId, botRequest.CreateTime, botRequest.OfficeId,
		botRequest.PatientId, botRequest.Status, resultJSON, docID, errorName, errorStack,
	)
	if err != nil {
		log.Printf("❌ Failed to insert/update SQLite: %v", err)
	} else {
		log.Printf("✅ Successfully inserted/updated record for Document ID: %s", docID)
	}
}

func trackAcknowledgedDocument(db *sql.DB, botRequest BotRequest, docID string, errorName string, errorStack string) error {
	resultJSON, err := json.Marshal(botRequest.Result)
	if err != nil {
		return err
	}

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM acknowledged_documents WHERE correlationid = ?", botRequest.CorrelationId).Scan(&count)
	if err != nil {
		return err
	}

	if count > 0 {
		log.Printf("🔄 Record with correlationId %s already exists in acknowledged_documents", botRequest.CorrelationId)
		return nil
	}

	query := `
        INSERT INTO acknowledged_documents (docid, botid, correlationid, createtime, officeid, patientid, status, result, error, stack)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    `
	_, err = db.Exec(query,
		docID, botRequest.BotId, botRequest.CorrelationId, botRequest.CreateTime, botRequest.OfficeId,
		botRequest.PatientId, botRequest.Status, resultJSON, errorName, errorStack,
	)
	if err != nil {
		return err
	}
	return nil
}

func deleteAcknowledgedDocument(db *sql.DB, correlationId string) {
	query := `DELETE FROM acknowledged_documents WHERE correlationid = ?`
	_, err := db.Exec(query, correlationId)
	if err != nil {
		log.Printf("❌ Failed to delete acknowledged documents with correlationId %s: %v", correlationId, err)
	} else {
		log.Printf("🗑️ Successfully deleted acknowledged documents with correlationId: %s", correlationId)
	}
}

func extractErrors(ctx context.Context, docRef *firestore.DocumentRef) (string, string) {
	iter := docRef.Collection("Errors").Documents(ctx)
	defer iter.Stop()

	var errorName, errorStack string
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Printf("❌ Error retrieving Errors subcollection: %v", err)
			break
		}

		data := doc.Data()
		if name, ok := data["name"].(string); ok {
			errorName = name
		}
		if stack, ok := data["stack"].(string); ok {
			errorStack = stack
		}
	}
	return errorName, errorStack
}
