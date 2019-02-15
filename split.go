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

type Phone struct {
	TelefonoID string
	ClientID   string
	Number     string
	City       string
	Country    string
	Company    string
}

func main() {
	splitCalls(os.Args[1])
	splitPhones(os.Args[2])
}

func splitCalls(callSourcePath string) {
	processedCallIds := make(map[string]bool)
	// Open CSV file
	inputFile, err := os.Open(callSourcePath)
	if err != nil {
		panic(err)
	}
	defer inputFile.Close()

	edgesFile, err := os.Create("callEdges.csv")
	checkError("Cannot create file", err)
	defer edgesFile.Close()

	edgesWriter := csv.NewWriter(edgesFile)
	defer edgesWriter.Flush()

	callVerticesFile, err := os.Create("callVertices.csv")
	checkError("Cannot create file", err)
	defer callVerticesFile.Close()

	callVerticesWriter := csv.NewWriter(callVerticesFile)
	defer callVerticesWriter.Flush()

	// Write headers
	callVerticesWriter.WriteAll([][]string{
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
			callVerticesWriter.Write([]string{"llamada", call.ID, call.DateTime, call.Duration})
			edgesWriter.Write([]string{"telefonoId", "numeric", call.CallerID})
			edgesWriter.Write([]string{"llamadaId", "numeric", call.ID})
			edgesWriter.Write([]string{"out", "creo"})
		}

		edgesWriter.Write([]string{"telefonoId", "numeric", call.ReceiverID})
		edgesWriter.Write([]string{"llamadaId", "numeric", call.ID})
		edgesWriter.Write([]string{"out", "participo_en"})
		processedCallIds[call.ID] = true
	}
}

func splitPhones(phoneSourcePath string) {
	processedClientIds := make(map[string]bool)
	// Open CSV file
	inputFile, err := os.Open(phoneSourcePath)
	if err != nil {
		panic(err)
	}
	defer inputFile.Close()

	edgesFile, err := os.Create("hasPhoneEdges.csv")
	checkError("Cannot create file", err)
	defer edgesFile.Close()

	edgesWriter := csv.NewWriter(edgesFile)
	defer edgesWriter.Flush()

	clientVerticesFile, err := os.Create("clientVertices.csv")
	checkError("Cannot create file", err)
	defer clientVerticesFile.Close()

	clientVerticesWriter := csv.NewWriter(clientVerticesFile)
	defer clientVerticesWriter.Flush()

	// Write headers
	clientVerticesWriter.WriteAll([][]string{
		{"type", "clientId"},
		{"label", "numeric"},
	})

	phoneVerticesFile, err := os.Create("phoneVertices.csv")
	checkError("Cannot create file", err)
	defer phoneVerticesFile.Close()

	phoneVerticesWriter := csv.NewWriter(phoneVerticesFile)
	defer phoneVerticesWriter.Flush()

	// Write headers
	phoneVerticesWriter.WriteAll([][]string{
		{"type", "telefonoId", "number", "city", "country", "company"},
		{"label", "numeric", "string", "string", "string", "string"},
	})

	reader := csv.NewReader(bufio.NewReader(inputFile))

	for {
		line, error := reader.Read()
		if error == io.EOF {
			break
		} else if error != nil {
			log.Fatal(error)
		}
		phone := Phone{
			TelefonoID: strings.Trim(line[0], " "),
			ClientID:   strings.Trim(line[1], " "),
			Number:     strings.Trim(line[2], " "),
			City:       strings.Trim(line[3], " "),
			Country:    strings.Trim(line[4], " "),
			Company:    strings.Trim(line[5], " "),
		}
		if !processedClientIds[phone.ClientID] {
			clientVerticesWriter.Write([]string{"cliente", phone.ClientID})
		}

		phoneVerticesWriter.Write([]string{"telefono", phone.TelefonoID, phone.Number, phone.City, phone.Country, phone.Company})

		edgesWriter.Write([]string{"clientId", "numeric", phone.ClientID})
		edgesWriter.Write([]string{"telefonoId", "numeric", phone.TelefonoID})
		edgesWriter.Write([]string{"out", "tiene_telefono"})
		processedClientIds[phone.ClientID] = true
	}
}

func checkError(message string, err error) {
	if err != nil {
		log.Fatal(message, err)
	}
}
