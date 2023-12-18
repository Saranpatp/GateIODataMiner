package main

import (
	"archive/tar"
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type CumulativeAmounts struct {
	BuyAmount  float64
	SellAmount float64
	SetAmount float64
	BeginID    string
}

type BBO struct {
	BestBid float64
	BuyAmount float64
	BestAsk float64
	SellAmount float64
	BeginID string
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
	semaphore := make(chan struct{}, 8)

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
	dirPath := fmt.Sprintf("parsed_data")
	tarGzPath := "parsed_data.tar.gz"

	err = compressToTarGz(dirPath, tarGzPath)
	if err != nil {
		log.Fatalf("Error compressing directory: %v", err)
	}
	log.Println("Directory compressed successfully.")

}
func processTicker(tickerFolderPath string, tickername string) {
	dir := tickerFolderPath + "/spot/orderbooks" // Replace with your directory path

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
		log.Printf("Failed To decompressed %s : %s", filePath, err)
		return
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
			log.Printf("Fail to read the record %s", err)
			continue
		}

		timestamp := record[colsMapping["timestamp"]]
		action := record[colsMapping["action"]]
		amountStr := record[colsMapping["amount"]]
		priceStr := record[colsMapping["price"]]
		beginId := record[colsMapping["begin_id"]]

		// // Ignore if action is "set"
		if action == "set" {
			continue
		}

		amount, err := strconv.ParseFloat(amountStr, 64)
		if err != nil {
			log.Println("Error parsing amount to float :", err)
			continue
		}

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
		case "set":
			amountsMap[timestamp][priceStr].SetAmount += amount
		}
		
	}
	err = ssFormatter(&amountsMap, foldername, tickername)
	if err != nil {
		log.Fatal(err)
	}

}

// Dont parse price just yet for prevent floating point error
func ssFormatter(amountsMap *map[string]map[string]*CumulativeAmounts, foldername string, tickername string) error {

	// Ensure the directory exists
	dirPath := fmt.Sprintf("parsed_data")
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
	modedTickerName := strings.Replace(tickername, "_", "", -1)
	saveFilePath := fmt.Sprintf("%s/tick_%s_%s.txt", dirPath, modedTickerName, formattedDate)

	file, err := os.OpenFile(saveFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	var bbo BBO


	for timestampStr, priceMap := range *amountsMap {

				// parser time
		// collectionTime := fmt.Sprintf("%s.%06d", time.Now().Format("2006-01-02 15:04:05"), 0)
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
		const epsilon = 0.00000001
		
		var ssFormattedStr string
		// var prev_bbo float64

		for price, amounts := range priceMap {

			floatPrice, err := strconv.ParseFloat(price,64)
			if err != nil {
				return err
			}

			// if math.Abs(amounts.BuyAmount - 0) > epsilon && math.Abs(amounts.BuyAmount - 0) > epsilon {
			// 	// prev_bbo, err = strconv.ParseFloat(price,64) // set as bbo
			// 	// if err != nil {
			// 	// 	return err
			// 	// }

			// 	ssFormattedStr = fmt.Sprintf("%s,%s,%s,%c,%s,%d,%s,%f, %s, %f,,", sourceTime, sourceTime, amounts.BeginID, 'Q', "IEX", 1, price,amounts.BuyAmount, price, amounts.SellAmount)
			// 	if _, err := file.WriteString(ssFormattedStr + "\n"); err != nil {
			// 		return err
			// 	}
			// }

			if math.Abs(amounts.BuyAmount - 0.0) > epsilon {
				ssFormattedStr = fmt.Sprintf("%s,%s,%s,%c,%s,%d,%s,%f,,,,0", sourceTime, sourceTime, amounts.BeginID, 'P', "IEX", 1, price, amounts.BuyAmount)
				if _, err := file.WriteString(ssFormattedStr + "\n"); err != nil {
					return err
				}
				if (floatPrice > bbo.BestBid){
					bbo.BestBid = floatPrice
					bbo.BuyAmount = amounts.BuyAmount
					bbo.BeginID = amounts.BeginID
				}
				
			}
			if math.Abs(amounts.SellAmount - 0.0) > epsilon {
				ssFormattedStr = fmt.Sprintf("%s,%s,%s,%c,%s,%d,%s,%f,,,,0", sourceTime, sourceTime, amounts.BeginID, 'P', "IEX", 2, price, amounts.SellAmount)
				if _, err := file.WriteString(ssFormattedStr + "\n"); err != nil {
					return err
				}
				if (bbo.BestAsk == 0.0 || bbo.BestAsk < floatPrice){
					bbo.BestAsk = floatPrice
					bbo.SellAmount = amounts.SellAmount
					bbo.BeginID = amounts.BeginID
				}
			}
		}

		// process bbo
		ssFormattedStr = fmt.Sprintf("%s,%s,%s,%c,%s,%d,%f,%f, %f, %f,,", sourceTime, sourceTime, bbo.BeginID, 'Q', "IEX", 1, bbo.BestBid,bbo.BuyAmount, bbo.BestAsk, bbo.SellAmount)
			if _, err := file.WriteString(ssFormattedStr + "\n"); err != nil {
				return err
		}
	}

	return nil

}

func compressToTarGz(srcDir, tarGzPath string) error {
	// Create output file
	outFile, err := os.Create(tarGzPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	// Set up the gzip writer
	gzipWriter := gzip.NewWriter(outFile)
	defer gzipWriter.Close()

	// Set up the tar writer
	tarWriter := tar.NewWriter(gzipWriter)
	defer tarWriter.Close()

	// Walk through the source directory
	err = filepath.Walk(srcDir, func(file string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Create a header
		header, err := tar.FileInfoHeader(fi, fi.Name())
		if err != nil {
			return err
		}
		header.Name = filepath.ToSlash(file)

		// Write file header
		if err := tarWriter.WriteHeader(header); err != nil {
			return err
		}
		// If it's not a directory, write file content
		if !fi.Mode().IsDir() {
			file, err := os.Open(file)
			if err != nil {
				return err
			}
			defer file.Close()
			_, err = io.Copy(tarWriter, file)
			return err
		}
		return nil
	})

	return err
}
