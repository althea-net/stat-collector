package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/fabioberger/airtable-go"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Settings struct {
	AirtableAPIKey    string
	AirtableBaseID    string
	AirtableTableName string
	GraylogURL        string
	GraylogUser       string
	GraylogPass       string
	From              time.Time
	To                time.Time
	Duration          time.Duration
	MongoDatabase     string
	MongoCollection   string
	MongoURL          string
}

type MeshMember struct {
	ID     string
	Fields struct {
		Name     string
		WGKey    string `json:"WG Key"`
		Upstream []string
	}
}

type BandwidthUsagePeriod struct {
	Name     string
	From     time.Time
	To       time.Time
	Duration time.Duration
	Up       *float64
	Down     *float64
	Total    *float64
}

// init is invoked before main()
func init() {
	// loads values from .env into the system
	if err := godotenv.Load(); err != nil {
		log.Print("No .env file found")
	}
}

func bytesToGb(bytes float64) float64 {
	return bytes / 1000000000
}

func callGraylog(settings Settings, direction string, wgKey string) *float64 {
	graylogClient := http.Client{
		Timeout: time.Second * 60,
	}

	var directionString string

	if direction == "up" {
		directionString = "uploaded to exit"
	} else if direction == "down" {
		directionString = "downloaded from exit"
	} else {
		log.Fatal("invalid direction argument")
	}

	params := url.Values{
		"field": []string{"bytes"},
		"query": []string{`"` + wgKey + `" AND "` + directionString + `"`},
		"from":  []string{settings.From.Format("2006-01-2T15:04:05.000Z")},
		"to":    []string{settings.To.Format("2006-01-2T15:04:05.000Z")},
	}

	url := strings.Replace(settings.GraylogURL+"api/search/universal/absolute/stats?"+params.Encode(), "+", "%20", -1)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		log.Fatal(err)
	}

	req.SetBasicAuth(settings.GraylogUser, settings.GraylogPass)
	req.Header.Add("Accept", "application/json")

	resp, err := graylogClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	bodyText, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}

	type GraylogRes struct {
		Sum *float64 `json:"sum"`
	}

	bodyText = bytes.Replace(bodyText, []byte(`"NaN"`), []byte(`null`), -1)

	var graylogRes GraylogRes
	err = json.Unmarshal(bodyText, &graylogRes)
	if err != nil {
		fmt.Println("error:", err)
	}

	if graylogRes.Sum != nil {
		sum := bytesToGb(*graylogRes.Sum)
		return &sum
	} else {
		return nil
	}
}

func getMeshMembers(settings Settings) ([]MeshMember, error) {
	// Get mesh members from airtable
	meshMembers := []MeshMember{}

	client, err := airtable.New(settings.AirtableAPIKey, settings.AirtableBaseID)
	if err != nil {
		return meshMembers, err
	}

	if err := client.ListRecords(settings.AirtableTableName, &meshMembers); err != nil {
		return meshMembers, err
	}

	return meshMembers, nil
}

func getBWUPCollection(settings Settings) (*mongo.Collection, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(settings.MongoURL))
	if err != nil {
		return nil, err
	}

	return mongoClient.Database(settings.MongoDatabase).Collection(settings.MongoCollection), nil
}

func getBandwidthSums(settings Settings, member MeshMember) (sumUploaded *float64, sumDownloaded *float64, total *float64) {
	sumDownloaded = callGraylog(settings, "down", member.Fields.WGKey)
	sumUploaded = callGraylog(settings, "up", member.Fields.WGKey)

	// We are using a nil pointer on these bandwidth sums as a very janky "Maybe" enum
	userIsActive := false
	var sumTotal float64

	if sumDownloaded != nil {
		userIsActive = true
		sumTotal += *sumDownloaded
	}

	if sumUploaded != nil {
		userIsActive = true
		sumTotal += *sumUploaded
	}

	// We want to leave this as nil if the user was not active
	if userIsActive {
		total = &sumTotal
	}

	return sumUploaded, sumDownloaded, total
}

func main() {
	// Configure settings
	duration, err := time.ParseDuration(os.Args[1])

	var to time.Time
	if len(os.Args) == 2 {
		to = time.Now()
	} else {
		to, err = time.Parse("2006-01-2T15:04:05", os.Args[2]+"T00:00:00")
	}

	from := to.Add(-duration)

	if err != nil {
		errString := `Usage: $ stat-collector duration [end_time]
		
		duration must be formatted like 168h
		
		end_time must be formatted like 2006-01-2T15:04:05. If no end_time is supplied,
		it will use the current time.`

		if err != nil {
			errString = errString + `

			error: ` + fmt.Sprintf("%v", err)
		}
		log.Fatal(errString)
	}

	settings := Settings{
		AirtableAPIKey:    os.Getenv("AIRTABLE_API_KEY"),
		AirtableBaseID:    os.Getenv("AIRTABLE_BASE_ID"),
		AirtableTableName: os.Getenv("AIRTABLE_TABLE_NAME"),
		GraylogURL:        os.Getenv("GRAYLOG_URL"),
		GraylogUser:       os.Getenv("GRAYLOG_USER"),
		GraylogPass:       os.Getenv("GRAYLOG_PASS"),
		From:              from,
		To:                to,
		Duration:          duration,
		MongoDatabase:     os.Getenv("MONGO_DATABASE"),
		MongoCollection:   os.Getenv("MONGO_COLLECTION"),
		MongoURL:          os.Getenv("MONGO_URL"),
	}

	fmt.Println(settings)

	meshMembers, err := getMeshMembers(settings)
	if err != nil {
		log.Fatal(err)
	}

	bwupCollection, err := getBWUPCollection(settings)
	if err != nil {
		log.Fatal(err)
	}

	// Loop which calls graylog, processes data, and saves and prints it
	for _, member := range meshMembers {
		sumUploaded, sumDownloaded, total := getBandwidthSums(settings, member)

		// Save bandwidth usage in mongo
		if total != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			bwup := BandwidthUsagePeriod{
				Name:     strings.TrimSpace(member.Fields.Name),
				From:     settings.From,
				To:       settings.To,
				Duration: settings.Duration,
				Up:       sumUploaded,
				Down:     sumDownloaded,
				Total:    total,
			}

			jsonBwup, _ := json.Marshal(bwup)

			fmt.Println(string(jsonBwup))

			_, err = bwupCollection.InsertOne(ctx, bwup)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}
