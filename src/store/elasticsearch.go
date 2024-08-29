package store

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
)

type Store interface {
	GetPlaces(limit int, offset int) ([]map[string]interface{}, int, error)
	GetNearestPlaces(lat float64, lon float64, limit int) ([]map[string]interface{}, error)
}

type ElasticsearchStore struct {
	client *elasticsearch.Client
}

func NewElasticsearchStore(client *elasticsearch.Client) *ElasticsearchStore {
	return &ElasticsearchStore{client: client}
}

func (es *ElasticsearchStore) GetPlaces(limit int, offset int) ([]map[string]interface{}, int, error) {
	var buf strings.Builder
	query := map[string]interface{}{
		"from": offset,
		"size": limit,
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
	}
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, 0, fmt.Errorf("Error encoding query: %s", err)
	}

	reader := strings.NewReader(buf.String())
	searchResponse, err := es.client.Search(
		es.client.Search.WithContext(context.Background()),
		es.client.Search.WithIndex("places"),
		es.client.Search.WithBody(reader),
		es.client.Search.WithTrackTotalHits(true),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("Error getting response: %s", err)
	}
	defer searchResponse.Body.Close()

	var searchResult map[string]interface{}
	if err := json.NewDecoder(searchResponse.Body).Decode(&searchResult); err != nil {
		return nil, 0, fmt.Errorf("Error parsing the response body: %s", err)
	}

	hits := searchResult["hits"].(map[string]interface{})["hits"].([]interface{})
	places := make([]map[string]interface{}, len(hits))
	for i, hit := range hits {
		source := hit.(map[string]interface{})["_source"]
		places[i] = source.(map[string]interface{})
	}

	total := int(searchResult["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64))

	return places, total, nil
}

func (es *ElasticsearchStore) GetNearestPlaces(lat float64, lon float64, limit int) ([]map[string]interface{}, error) {
	var buf strings.Builder
	query := map[string]interface{}{
		"size": limit,
		"sort": []map[string]interface{}{
			{
				"_geo_distance": map[string]interface{}{
					"location":        map[string]float64{"lat": lat, "lon": lon},
					"order":           "asc",
					"unit":            "km",
					"mode":            "min",
					"distance_type":   "arc",
					"ignore_unmapped": true,
				},
			},
		},
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
	}
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, fmt.Errorf("Error encoding geo query: %s", err)
	}

	reader := strings.NewReader(buf.String())
	searchResponse, err := es.client.Search(
		es.client.Search.WithContext(context.Background()),
		es.client.Search.WithIndex("places"),
		es.client.Search.WithBody(reader),
	)
	if err != nil {
		return nil, fmt.Errorf("Error getting geo response: %s", err)
	}
	defer searchResponse.Body.Close()

	var searchResult map[string]interface{}
	if err := json.NewDecoder(searchResponse.Body).Decode(&searchResult); err != nil {
		return nil, fmt.Errorf("Error parsing the geo response body: %s", err)
	}

	hits := searchResult["hits"].(map[string]interface{})["hits"].([]interface{})
	places := make([]map[string]interface{}, len(hits))
	for i, hit := range hits {
		source := hit.(map[string]interface{})["_source"]
		places[i] = source.(map[string]interface{})
	}

	return places, nil
}

func CreateIndex(es *elasticsearch.Client) error {
	mapping := `{
		"mappings": {
			"properties": {
				"id": {"type": "keyword"},
				"name": {"type": "text"},
				"address": {"type": "text"},
				"phone": {"type": "text"},
				"location": {"type": "geo_point"}
			}
		}
	}`

	res, err := es.Indices.Create("places", es.Indices.Create.WithBody(strings.NewReader(mapping)))
	if err != nil {
		return fmt.Errorf("error creating index: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("error creating index: %s", res.String())
	}
	return nil
}

func LoadData(es *elasticsearch.Client, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(bufio.NewReader(file))
	reader.Comma = '\t'

	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("error reading CSV: %w", err)
	}

	var buffer bytes.Buffer
	for i, record := range records {
		if i == 0 {
			continue // Skip header
		}
		doc := map[string]interface{}{
			"id":      record[0],
			"name":    record[1],
			"address": record[2],
			"phone":   record[3],
			"location": map[string]float64{
				"lat": parseFloat(record[5]),
				"lon": parseFloat(record[4]),
			},
		}
		meta := []byte(fmt.Sprintf(`{ "index" : { "_index" : "places", "_id" : "%s" } }%s`, record[0], "\n"))
		data, _ := json.Marshal(doc)
		buffer.Write(meta)
		buffer.Write(data)
		buffer.WriteString("\n")
	}

	res, err := es.Bulk(bytes.NewReader(buffer.Bytes()))
	if err != nil {
		return fmt.Errorf("error executing bulk request: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("bulk request error: %s", res.String())
	}
	return nil
}

func parseFloat(s string) float64 {
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

func DeleteIndexIfExists(es *elasticsearch.Client, indexName string) error {
	res, err := es.Indices.Delete([]string{indexName})
	if err != nil {
		return fmt.Errorf("error deleting index: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("error deleting index: %s", res.String())
	}
	return nil
}
