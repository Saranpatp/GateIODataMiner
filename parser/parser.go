package main

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type CumulativeAmounts struct {
	BuyAmount  float64
	SellAmount float64
	BeginID    string
}

func main() {
	dir := "data" //BTC_USDT/spot/orderbooks" // Replace with your directory path

	// Read the directory contents
	entries, err := os.ReadDir(dir)
	if err != nil {
		log.Fatal(err)
	}

	// Filter for directories (subfolders)
	var tickerFolder []string
	for _, entry := range entries {
		if entry.IsDir() {
			tickerFolder = append(tickerFolder, entry.Name())
		}
	}

	// Semaphore channel to limit goroutine
	semaphore := make(chan struct{}, 5)

	var wg sync.WaitGroup

	// Process each file
	for _, tickername := range tickerFolder {
		wg.Add(1)

		semaphore <- struct{}{}

		go func(tickerName string) {
			defer wg.Done()

			fullPath := dir + "/" + tickerName
			processTicker(fullPath, tickerName)

			<-semaphore
		}(tickername)
	}

	wg.Wait()
}
func processTicker(tickerFolderPath string, tickername string) {
	dir := tickerFolderPath+"/spot/orderbooks" // Replace with your directory path

	// Read the directory contents
	entries, err := os.ReadDir(dir)
	if err != nil {
		log.Fatal(err)
	}

	// Filter for directories (subfolders)
	var folders []string
	for _, entry := range entries {
		if entry.IsDir() {
			folders = append(folders, entry.Name())
		}
	}

	sort.Strings(folders)

	// Process each file
	for _, foldername := range folders {
		fullPath := dir + "/" + foldername
		processFolder(fullPath, foldername, tickername)
	}
}

func processFolder(folderPath string, foldername string, tickername string) {
	files, err := os.ReadDir(folderPath)
	if err != nil {
		log.Fatal(err)
	}

	// Filter and sort files
	var csvGzFiles []string
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".csv.gz") {
			csvGzFiles = append(csvGzFiles, file.Name())
		}
	}
	sort.Strings(csvGzFiles)

	// Process each file
	for _, filename := range csvGzFiles {
		fullPath := folderPath + "/" + filename
		processFile(fullPath, foldername, tickername)
	}
}

func processFile(filePath string, foldername string, tickername string) {
	// For demonstration purposes, just printing the file name
	log.Printf("Processing file: %s\n", filePath)

	colsMapping := map[string]int{
		"timestamp": 0,
		"side":      1,
		"action":    2,
		"price":     3,
		"amount":    4,
		"begin_id":  5,
		"merged":    6,
	}
	// Open the gzip file
	gzFile, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer gzFile.Close()

	// Decompress the gzip file
	gzReader, err := gzip.NewReader(gzFile)
	if err != nil {
		log.Fatal(err)
	}
	defer gzReader.Close()

	// Read from the gzReader
	// Implement the CSV reading and processing logic here
	// Read the CSV contents
	csvReader := csv.NewReader(gzReader)

	amountsMap := make(map[string]map[string]*CumulativeAmounts)
	for {
		record, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				break // End of file
			}
			log.Fatal(err)
		}

		timestamp := record[colsMapping["timestamp"]]
		action := record[colsMapping["action"]]
		amountStr := record[colsMapping["amount"]]
		priceStr := record[colsMapping["price"]]
		beginId := record[colsMapping["begin_id"]]

		// Ignore if action is "set"
		if action == "set" {
			continue
		}

		amount, err := strconv.ParseFloat(amountStr, 64)
		if err != nil {
			fmt.Println("Error parsing amount to float :", err)
			continue
		}

		// Check if the timestamp map exists, if not, create it
		if _, exists := amountsMap[timestamp]; !exists {
			amountsMap[timestamp] = make(map[string]*CumulativeAmounts)

			// Check if the timestamp map exists, if not, create it
			if _, exists := amountsMap[timestamp]; !exists {
				amountsMap[timestamp] = make(map[string]*CumulativeAmounts)
			}

			// Check if the price map exists for the timestamp, if not, create it
			if _, exists := amountsMap[timestamp][priceStr]; !exists {
				amountsMap[timestamp][priceStr] = &CumulativeAmounts{}
			}
			// Add begin id
			amountsMap[timestamp][priceStr].BeginID = beginId
			// Update sell or buy amount
			switch action {
			case "make":
				amountsMap[timestamp][priceStr].BuyAmount += amount
			case "take":
				amountsMap[timestamp][priceStr].SellAmount += amount
			}
		}
	}
	// TODO: add formatter here
	err = ssFormatter(&amountsMap, foldername, tickername)
	if err != nil {
		log.Fatal(err)
	}

	

}

// Dont parse price just yet for prevent floating point error
func ssFormatter(amountsMap *map[string]map[string]*CumulativeAmounts, foldername string, tickername string) error {

	// Ensure the directory exists
	dirPath := fmt.Sprintf("parsed_data/%s", tickername)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return err
	}

	//formated date for save filename
	t, err := time.Parse("2006-01-02", foldername)
	if err != nil {
		fmt.Println("Error parsing date:", err)
		return err
	}
	formattedDate := t.Format("20060102")
	saveFilePath := fmt.Sprintf("%s/tick_%s_%s.txt", dirPath, tickername, formattedDate)

	file, err := os.OpenFile(saveFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	for timestampStr, priceMap := range *amountsMap {
		for price, amounts := range priceMap {
			// parser time
			collectionTime := fmt.Sprintf("%s.%06d", time.Now().Format("2006-01-02 15:04:05"), 0)

			floatSourceTime, err := strconv.ParseFloat(timestampStr, 64)
			if err != nil {
				return err
			}
			intSourceTime := int64(floatSourceTime)

			// Convert Unix timestamp to time.Time
			t := time.Unix(intSourceTime, 0)

			// Format as string (YYYY-MM-DD HH:MM:SS.microseconds)
			// Handle microseconds
			microseconds := int64((floatSourceTime - float64(intSourceTime)) * 1e6)
			sourceTime := fmt.Sprintf("%s.%06d", t.UTC().Format("2006-01-02 15:04:05"), microseconds)

			var ssFormattedStr string
			if amounts.BuyAmount != 0 {
				ssFormattedStr = fmt.Sprintf("%s,%s,%s,%c,%s,%d,%s,%f", collectionTime, sourceTime, amounts.BeginID, 'T', "gate", 1, price, amounts.BuyAmount)
				if _, err := file.WriteString(ssFormattedStr + "\n"); err != nil {
					return err
				}
			}
			if amounts.SellAmount != 0 {
				ssFormattedStr = fmt.Sprintf("%s,%s,%s,%c,%s,%d,%s,%f", collectionTime, sourceTime, amounts.BeginID, 'T', "gate", 2, price, amounts.SellAmount)
				if _, err := file.WriteString(ssFormattedStr + "\n"); err != nil {
					return err
				}
			}
		}
	}

	return nil

}
