package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/joho/godotenv"
)

var (
	client    *http.Client
	wg        sync.WaitGroup
	semaphore chan struct{}
)

func init() {
	client = &http.Client{}
	semaphore = make(chan struct{}, 8) // Buffered channel with capacity of 5
}

func downloadFile(url, filepath string) error {
	defer wg.Done()
	defer func() { <-semaphore }() // Release semaphore after download is complete

	// Get the data
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Create the file
	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	// Buffered writing
	bufWriter := bufio.NewWriter(out)
	defer bufWriter.Flush()

	// Write the body to file
	_, err = io.Copy(bufWriter, resp.Body)
	return err
}

func main() {
	// Load .env file
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	startDateStr := os.Getenv("START_DATE")
	endDateStr := os.Getenv("END_DATE")
	markets := strings.Split(os.Getenv("MARKETS"), ",")

	baseurl := "https://download.gatedata.org/"
	bizTypes := []string{"spot", "futures_usdt"}

	bizTypeToDataTypes := map[string][]string{
		"spot":         {"orderbooks"},// {"deals", "orderbooks", "candlesticks"},
		// "futures_usdt": {"trades", "orderbooks", "mark_prices", "funding_updates", "funding_applies"},
	}

	start, _ := time.Parse("2006-01-02", startDateStr)
	end, _ := time.Parse("2006-01-02", endDateStr)

	for _, market := range markets {
		marketDir := filepath.Join("data", market)
		if err := os.MkdirAll(marketDir, os.ModePerm); err != nil {
			log.Fatalf("Error creating market directory %s: %v", marketDir, err)
		}

		for _, biz := range bizTypes {
			bizDir := filepath.Join(marketDir, biz)
			if err := os.MkdirAll(bizDir, os.ModePerm); err != nil {
				log.Fatalf("Error creating biz directory %s: %v", bizDir, err)
			}

			dataTypes, ok := bizTypeToDataTypes[biz]
			if !ok {
				log.Printf("No data types defined for bizType: %s", biz)
				continue
			}

			for _, dataType := range dataTypes {
				dataTypeDir := filepath.Join(bizDir, dataType)
				if err := os.MkdirAll(dataTypeDir, os.ModePerm); err != nil {
					log.Fatalf("Error creating data type directory %s: %v", dataTypeDir, err)
				}

				for d := start; d.Before(end); d = d.AddDate(0, 0, 1) {
					year := d.Year()
					month := fmt.Sprintf("%02d", d.Month())
					day := fmt.Sprintf("%02d", d.Day())

					// Create a directory for each day
					dailyDir := filepath.Join(dataTypeDir, fmt.Sprintf("%d-%s-%s", year, month, day))
					if err := os.MkdirAll(dailyDir, os.ModePerm); err != nil {
						log.Fatalf("Error creating daily directory %s: %v", dailyDir, err)
					}

					if dataType == "orderbooks" {
						for hour := 0; hour < 24; hour++ {
							wg.Add(1)
							semaphore <- struct{}{} // Acquire a slot in the semaphore

							go func(hour int, dataType string, biz string, market string) {
								hourStr := fmt.Sprintf("%02d", hour)
								url := fmt.Sprintf("%s%s/%s/%d%s/%s-%d%s%s%s.csv.gz", baseurl, biz, dataType, year, month, market, year, month, day, hourStr)
								filepath := filepath.Join(dailyDir, fmt.Sprintf("%s-%s-%d%s%s%s.csv.gz", market, dataType, year, month, day, hourStr))

								fmt.Println("Downloading", url)
								if err := downloadFile(url, filepath); err != nil {
									fmt.Println("Error downloading file:", err)
								}
							}(hour, dataType, biz, market)
						}
					} else {
						wg.Add(1)
						semaphore <- struct{}{} // Acquire a slot in the semaphore
						go func(dataType string, biz string, market string) {
							url := fmt.Sprintf("%s%s/%s/%d%s/%s-%d%s.csv.gz", baseurl, biz, dataType, year, month, market, year, month)
							filepath := filepath.Join(dataTypeDir, fmt.Sprintf("%s-%s-%d%s.csv.gz", market, dataType, year, month))

							fmt.Println("Downloading", url)
							if err := downloadFile(url, filepath); err != nil {
								fmt.Println("Error downloading file:", err)
							}
						}(dataType, biz, market)
					}
				}
			}
		}
	}
	wg.Wait()
}
