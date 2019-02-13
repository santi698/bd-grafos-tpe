package main

import (
	"bufio"
	"encoding/csv"
	"io"
	"log"
	"os"
	"strings"
)

type Call struct {
	ID         string
	DateTime   string
	Duration   string
	CallerID   string
	ReceiverID string
}

func main() {
	processedCallIds := make(map[string]bool)
	// Open CSV file
	inputFile, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}
	defer inputFile.Close()

	edgesFile, err := os.Create("callEdges.csv")
	checkError("Cannot create file", err)
	defer edgesFile.Close()

	edgesWriter := csv.NewWriter(edgesFile)
	defer edgesWriter.Flush()

	verticesFile, err := os.Create("callVertices.csv")
	checkError("Cannot create file", err)
	defer verticesFile.Close()

	verticesWriter := csv.NewWriter(verticesFile)
	defer verticesWriter.Flush()

	// Write headers
	verticesWriter.WriteAll([][]string{
		{"type", "llamadaId", "timestamp", "duracion"},
		{"label", "numeric", "date", "numeric"},
	})

	reader := csv.NewReader(bufio.NewReader(inputFile))

	for {
		line, error := reader.Read()
		if error == io.EOF {
			break
		} else if error != nil {
			log.Fatal(error)
		}
		call := Call{
			ID:         strings.Trim(line[0], " "),
			DateTime:   strings.Trim(line[1], " "),
			Duration:   strings.Trim(line[2], " "),
			CallerID:   strings.Trim(line[3], " "),
			ReceiverID: strings.Trim(line[4], " "),
		}
		if !processedCallIds[call.ID] {
			verticesWriter.Write([]string{"llamada", call.ID, call.DateTime, call.Duration})
			edgesWriter.Write([]string{"telefonoId", "numeric", call.CallerID})
			edgesWriter.Write([]string{"llamadaId", "numeric", call.ID})
			edgesWriter.Write([]string{"out", "creo"})
		}

		edgesWriter.Write([]string{"telefonoId", "numeric", call.ReceiverID})
		edgesWriter.Write([]string{"llamadaId", "numeric", call.ID})
		edgesWriter.Write([]string{"out", "participoEn"})
		processedCallIds[call.ID] = true
	}
}

func checkError(message string, err error) {
	if err != nil {
		log.Fatal(message, err)
	}
}
