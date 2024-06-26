package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
)

type Subscriber struct {
	FirstName   string `json:"firstName"`
	LastName    string `json:"lastName"`
	DateOfBirth string `json:"dateOfBirth"`
	GroupID     string `json:"groupId"`
	ID          string `json:"id"`
}

type Dependent struct {
	FirstName   string `json:"firstName"`
	LastName    string `json:"lastName"`
	DateOfBirth string `json:"dateOfBirth"`
}

type Payer struct {
	Name string `json:"name"`
}

type Appointment struct {
	DateTime string `json:"dateTime"`
	Type     string `json:"type"`
	Status   string `json:"status"`
}

type Coverage struct {
	Primary struct {
		PayerName    string `json:"payerName"`
		PolicyNumber string `json:"policyNumber"`
		GroupNumber  string `json:"groupNumber"`
	} `json:"primary"`
	Secondary *struct {
		PayerName    string `json:"payerName"`
		PolicyNumber string `json:"policyNumber"`
		GroupNumber  string `json:"groupNumber"`
	} `json:"secondary,omitempty"`
}

type PatientData struct {
	SubscriberFirstName string        `json:"subscriberFirstName"`
	SubscriberLastName  string        `json:"subscriberLastName"`
	Birthdate           string        `json:"birthdate"`
	GroupID             string        `json:"groupId"`
	Coverage            Coverage      `json:"coverage"`
	Appointments        []Appointment `json:"appointments"`
	Dependents          []Dependent   `json:"dependents,omitempty"`
}

func generateGUID() string {
	return fmt.Sprintf("%010d", uuid.New().ID())
}

func transformQueryResult(record string) (map[string]interface{}, error) {
	var result map[string]interface{}

	if err := json.Unmarshal([]byte(record), &result); err != nil {
		fmt.Printf("Error decoding JSON: %v\n", err)
		return nil, err
	}

	return result, nil
}

func transformData(inputData map[string]interface{}) PatientData {
	currentDate := time.Now().Format(time.RFC3339)

	subscriber := inputData["subscriber"].(map[string]interface{})
	dependent, hasDependent := inputData["dependent"].(map[string]interface{})
	payer := inputData["payer"].(map[string]interface{})

	patientData := PatientData{
		SubscriberFirstName: subscriber["firstName"].(string),
		SubscriberLastName:  subscriber["lastName"].(string),
		Birthdate:           subscriber["dateOfBirth"].(string),
		GroupID:             subscriber["groupId"].(string),
		Coverage: Coverage{
			Primary: struct {
				PayerName    string `json:"payerName"`
				PolicyNumber string `json:"policyNumber"`
				GroupNumber  string `json:"groupNumber"`
			}{
				PayerName:    payer["name"].(string),
				PolicyNumber: subscriber["id"].(string),
				GroupNumber:  subscriber["groupId"].(string),
			},
		},
		Appointments: []Appointment{
			{
				DateTime: currentDate,
				Type:     "Routine Checkup",
				Status:   "Scheduled",
			},
		},
	}

	if _, hasSecondary := inputData["secondary"]; hasSecondary {
		patientData.Coverage.Secondary = &struct {
			PayerName    string `json:"payerName"`
			PolicyNumber string `json:"policyNumber"`
			GroupNumber  string `json:"groupNumber"`
		}{
			PayerName:    payer["name"].(string),
			PolicyNumber: subscriber["id"].(string),
			GroupNumber:  subscriber["groupId"].(string),
		}
	}

	if hasDependent {
		patientData.Dependents = []Dependent{
			{
				FirstName:   dependent["firstName"].(string),
				LastName:    dependent["lastName"].(string),
				DateOfBirth: dependent["dateOfBirth"].(string),
			},
		}
	}

	return patientData
}

func writeJSONFile(data interface{}, dir, fileName string) error {
	// Create directory if it doesn't exist
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return err
	}

	// Create file in the directory
	filePath := filepath.Join(dir, fileName)
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "    ")
	return encoder.Encode(data)
}

func fetchAndProcessData(db *sql.DB, botID string) ([]PatientData, error) {
	query := `SELECT result FROM main WHERE status = 'Succeeded' AND botid = ?`
	rows, err := db.Query(query, botID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var patients []PatientData
	for rows.Next() {
		var record string
		if err := rows.Scan(&record); err != nil {
			return nil, err
		}

		resultData, err := transformQueryResult(record)
		if err != nil {
			continue
		}

		patientData := transformData(resultData)
		patients = append(patients, patientData)
	}

	return patients, nil
}

func main() {
	botIDs := []string{
		"aetna-api", "cigna-api", "guardian-api", "ameritas", "humana-api",
		"metlife-data-bot", "dd-washington-json-api", "medicaid-of-washington",
		"umr", "anthem", "united-concordia-api", "medicaid-of-nevada-pdf",
		"bcbs-south-carolina", "geha-json-api-availity", "bcbs-california",
		"dentaquest-government-json-api", "bcbs-tennessee-json-api-availity",
		"bcbs-maryland", "bcbs-massachusetts",
	}

	connStr := "data.db"

	db, err := sql.Open("sqlite3", connStr)
	if err != nil {
		log.Fatalf("Error connecting to SQLite database: %v", err)
	}
	defer db.Close()

	dataDir := "data"
	for _, botID := range botIDs {
		patients, err := fetchAndProcessData(db, botID)
		if err != nil {
			log.Printf("Error fetching data for botid %s: %v", botID, err)
			continue
		}

		outputData := map[string]interface{}{
			"batchId":  fmt.Sprintf("batch_%s", generateGUID()),
			"patients": patients,
		}

		outputFile := fmt.Sprintf("%s_patients.json", botID)
		if err := writeJSONFile(outputData, dataDir, outputFile); err != nil {
			log.Printf("Error writing data to file %s: %v", outputFile, err)
			continue
		}

		fmt.Printf("Data for botid %s has been fetched, transformed, and written to %s.\n", botID, filepath.Join(dataDir, outputFile))
	}

	fmt.Println("All data has been fetched, transformed, and written to respective files.")
}
