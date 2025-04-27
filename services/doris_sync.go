package services

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"migration-tool-go/dtos/common"
	"migration-tool-go/dtos/destinations/doris"
	"migration-tool-go/logger"
	"migration-tool-go/utils"
	"net/http"
	"sync"
	"time"
)

var DorisSyncService = &dorisSyncService{}

type dorisSyncService struct {
	connectionDetails doris.ConnectionDetails
	configuration     doris.Configuration
}

func NewDorisSync(destination common.Destination[any]) {
	DorisSyncService = &dorisSyncService{
		connectionDetails: destination.Value.(doris.Doris).ConnectionDetails,
		configuration:     destination.Value.(doris.Doris).Configuration,
	}
}

func (d dorisSyncService) SyncDoris(records []map[string]any, noOfRecords uint64, table string, uniqueLabel string, checkAllRecordsProcessed *sync.Map, workerId uint32) error {
	// Convert records to JSON for destination
	jsonData, err := utils.ConvertRecordsToJSON(records, "", false)
	if err != nil {
		logger.Sugar.Errorf("Failed to convert records to JSON: %v", err)
		return err
	}

	// Construct the Doris URL
	beNodes := d.connectionDetails.BeNodes
	bePort := d.connectionDetails.BePort
	database := d.connectionDetails.Database
	tableName := table // Use the table name from the parameter

	dorisURL := fmt.Sprintf("http://%s:%d/api/%s/%s/_stream_load", beNodes, bePort, database, tableName)

	// Send the data to Doris
	err = d.StreamLoadDoris(
		dorisURL,
		d.connectionDetails.Username,
		d.connectionDetails.Password,
		jsonData,
		uniqueLabel,
		workerId,
		noOfRecords,
	)

	if err != nil {
		return err
	}

	// Update the processed records count using sync.Map
	currentCount, _ := checkAllRecordsProcessed.LoadOrStore(uniqueLabel, uint64(0))
	checkAllRecordsProcessed.Store(uniqueLabel, currentCount.(uint64)+noOfRecords)

	return nil
}

// StreamLoadDoris uploads JSON data directly to Apache Doris with robust retry logic
func (d dorisSyncService) StreamLoadDoris(dorisURL, username, password string, jsonData []byte, uniqueLabel string, workerId uint32, noOfRecords uint64) error {
	// Initialize retry parameters
	maxRetries := 10
	initialBackoff := 500 * time.Millisecond
	maxBackoff := 30 * time.Second
	backoff := initialBackoff
	attempt := 1

	for {
		st := time.Now()
		success := false
		var responseStatus string
		var responseBody string

		// Create HTTP request
		req, err := http.NewRequest("PUT", dorisURL, bytes.NewReader(jsonData))
		if err != nil {
			logger.Sugar.Errorf("Failed to create request (attempt %d): %v", attempt, err)
		} else {
			// Set Stream Load headers
			req.Header.Set("Expect", "100-continue")
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("format", "json")            // Specify JSON format
			req.Header.Set("strip_outer_array", "true") // Required for JSON array input
			req.Header.Set("label", uniqueLabel)        // Unique label
			req.Header.Set("send_batch_parallelism", "10")
			req.SetBasicAuth(username, password)

			// Send request with timeout
			client := &http.Client{
				Timeout: 5 * time.Minute, // Set a reasonable timeout
			}
			resp, err := client.Do(req)
			if err != nil {
				logger.Sugar.Errorf("Failed to send request (attempt %d): %v", attempt, err)
			} else {
				// Read response body
				body, err := io.ReadAll(resp.Body)
				resp.Body.Close() // Close body explicitly

				if err != nil {
					logger.Sugar.Errorf("Failed to read response body (attempt %d): %v", attempt, err)
				} else {
					responseStatus = resp.Status
					responseBody = string(body)

					// Check response status
					if resp.StatusCode == http.StatusOK {
						// Success!
						logger.Sugar.Infof("Worker %d, ✅ Doris Stream Load Successful for label %s, with %d records, took %f secs (attempt %d)",
							workerId, uniqueLabel, noOfRecords, time.Since(st).Seconds(), attempt)
						success = true
					} else {
						logger.Sugar.Errorf("Worker %d, Doris Stream load failed (attempt %d) for label %s with %d records: status %s, response %s, took %f secs",
							workerId, attempt, uniqueLabel, noOfRecords, responseStatus, responseBody, time.Since(st).Seconds())
					}
				}
			}
		}

		if success {
			return nil
		}

		// Handle retry logic
		// If we've reached max retries, continue retrying but log less frequently
		if attempt >= maxRetries {
			if attempt%5 == 0 {
				logger.Sugar.Warnf("Still retrying after %d attempts for label %s with %d records. Will continue until successful.",
					attempt, uniqueLabel, noOfRecords)
			}
		} else {
			logger.Sugar.Warnf("Retrying (attempt %d/%d) for label %s with %d records after backoff of %v",
				attempt, maxRetries, uniqueLabel, noOfRecords, backoff)
		}

		// Wait before retrying
		time.Sleep(backoff)

		// Increase backoff for next retry with exponential backoff and jitter
		jitter := time.Duration(float64(backoff) * 0.1 * (0.5 + rand.Float64())) // 10% jitter
		backoff = time.Duration(float64(backoff)*1.5) + jitter
		if backoff > maxBackoff {
			backoff = maxBackoff
		}

		attempt++
		// No maximum retry limit - we'll keep trying until successful
	}
}
