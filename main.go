package main

import (
    "fmt"
    "io"
    "log"
    "net/http"
    "os"
    "path/filepath"
    "strings"
    "time"

    "github.com/joho/godotenv"
)

func downloadFile(url string, filepath string) error {
    // Get the data
    resp, err := http.Get(url)
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

    // Write the body to file
    _, err = io.Copy(out, resp.Body)
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
	// dataTypes := []string{"deals", "orderbooks", "candlesticks", "trades", "mark_prices", "funding_updates", "funding_applies"}

	bizTypeToDataTypes := map[string][]string{
		"spot":         {"deals", "orderbooks", "candlesticks"},
		"futures_usdt": {"trades", "orderbooks", "mark_prices", "funding_updates", "funding_applies"},
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
			// doing type check here
			dataTypes, ok := bizTypeToDataTypes[biz]
			
			if !ok {
				log.Printf("No data types defined for bizType: %s", biz)
				continue
			}
            for _, dataType := range dataTypes {
				for d := start; d.Before(end); d = d.AddDate(0, 0, 1) { // Looping through days
					year := d.Year()
					month := fmt.Sprintf("%02d", d.Month())
					day := fmt.Sprintf("%02d", d.Day())
			
					if dataType == "orderbooks" {
						// Handle hourly logs for order books
						for hour := 0; hour < 24; hour++ {
							hourStr := fmt.Sprintf("%02d", hour)
							url := fmt.Sprintf("%s%s/%s/%d%s/%s-%d%s%s%s.csv.gz", baseurl, biz, dataType, year, month, market, year, month, day, hourStr)
							filepath := filepath.Join(bizDir, fmt.Sprintf("%s-%s-%d%s%s%s.csv.gz", market, dataType, year, month, day, hourStr))
			
							fmt.Println("Downloading", url)
							if err := downloadFile(url, filepath); err != nil {
								fmt.Println("Error downloading file:", err)
							}
						}
					} else {
						// Handle other data types
						url := fmt.Sprintf("%s%s/%s/%d%s/%s-%d%s.csv.gz", baseurl, biz, dataType, year, month, market, year, month)
						filepath := filepath.Join(bizDir, fmt.Sprintf("%s-%s-%d%s.csv.gz", market, dataType, year, month))
			
						fmt.Println("Downloading", url)
						if err := downloadFile(url, filepath); err != nil {
							fmt.Println("Error downloading file:", err)
						}
					}
				}
			}
        }
    }
}
