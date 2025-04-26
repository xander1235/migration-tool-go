package services

import (
	"bytes"
	"fmt"
	"io"
	"migration-tool-go/dtos/common"
	"migration-tool-go/dtos/destinations/doris"
	"migration-tool-go/logger"
	"net/http"
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

func (d dorisSyncService) SyncDoris(jsonData []byte, noOfRecords uint64, table string, uniqueLabel string, checkAllRecordsProcessed map[string]uint64, workerId uint32) error {
	// Step 2: Send JSON data directly to Doris (No file involved)
	dorisUrl := fmt.Sprintf("http://%s:%d/api/%s/%s/_stream_load", d.connectionDetails.BeNodes, d.connectionDetails.BePort, d.connectionDetails.Database, table)
	username := d.connectionDetails.Username
	password := d.connectionDetails.Password
	err := d.StreamLoadDoris(dorisUrl, username, password, jsonData, uniqueLabel, workerId, noOfRecords)

	if err != nil {
		return err
	}

	checkAllRecordsProcessed[uniqueLabel] = noOfRecords
	return nil
}

// StreamLoadDoris uploads JSON data directly to Apache Doris
func (d dorisSyncService) StreamLoadDoris(dorisURL, username, password string, jsonData []byte, uniqueLabel string, workerId uint32, noOfRecords uint64) error {
	st := time.Now()

	// Create HTTP request
	req, err := http.NewRequest("PUT", dorisURL, bytes.NewReader(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Set Stream Load headers
	req.Header.Set("Expect", "100-continue")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("format", "json")            // Specify JSON format
	req.Header.Set("strip_outer_array", "true") // Required for JSON array input
	req.Header.Set("label", uniqueLabel)        // Unique label
	req.Header.Set("send_batch_parallelism", "10")
	req.SetBasicAuth(username, password)

	// Send request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	//log the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}
	//logger.Sugar.Info(string(body))

	// Check response status
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Worker %d, Doris Stream load failed for label %s with no of records %d, with status %s response %s, and took time of %f secs", workerId, uniqueLabel, noOfRecords, resp.Status, string(body), time.Since(st).Seconds())
	}

	logger.Sugar.Infof("Worker %d, ✅ Doris Stream Load Successful for label %s, with no of records %d, and took time of %f secs", workerId, uniqueLabel, noOfRecords, time.Since(st).Seconds())
	return nil
}
