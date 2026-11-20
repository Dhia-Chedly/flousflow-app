package main

import (
    "net/http"
    "os"
    "time"
)

func main() {
    client := http.Client{Timeout: 3 * time.Second}
    response, err := client.Get("http://localhost:9000/minio/health/live")
    if err != nil {
        os.Exit(1)
    }
    defer response.Body.Close()
    if response.StatusCode != http.StatusOK {
        os.Exit(1)
    }
}
