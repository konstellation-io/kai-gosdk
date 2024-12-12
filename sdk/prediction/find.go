package prediction

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/konstellation-io/kai-gosdk/internal/common"
	"github.com/spf13/viper"
)

func (r *RedisPredictionStore) Find(ctx context.Context, filter *Filter) ([]Prediction, error) {
	var predictions []Prediction

	if err := filter.Validate(); err != nil {
		return nil, fmt.Errorf("invalid filter: %w", err)
	}

	result, err := r.client.Do(ctx, "FT.SEARCH", viper.GetString(common.ConfigRedisIndexKey), r.buildQueryWithFilters(filter)).Result()
	if err != nil {
		return nil, err
	}

	predictions, err = r.parseResultsToPredictionsList(result)
	if err != nil {
		return nil, err
	}

	return predictions, nil
}

func (r *RedisPredictionStore) buildQueryWithFilters(filter *Filter) string {
	queryFilters := []string{
		r.getSearchTagFilter("product", r.metadata.GetProduct()),
		r.getSearchNumericRangeFilter("creation_date", filter.CreationDate.StartDate, filter.CreationDate.EndDate),
	}

	if filter.Workflow != "" {
		queryFilters = append(queryFilters, r.getSearchTagFilter("workflow", filter.Workflow))
	}

	if filter.WorkflowType != "" {
		queryFilters = append(queryFilters, r.getSearchTagFilter("workflow_type", filter.WorkflowType))
	}

	if filter.Version != "" {
		queryFilters = append(queryFilters, r.getSearchTagFilter("version", filter.Version))
	} else {
		queryFilters = append(queryFilters, r.getSearchTagFilter("version", r.metadata.GetVersion()))
	}

	if filter.Process != "" {
		queryFilters = append(queryFilters, r.getSearchTagFilter("process", filter.Process))
	}

	if filter.RequestID != "" {
		queryFilters = append(queryFilters, r.getSearchTagFilter("request_id", filter.RequestID))
	}

	return strings.ReplaceAll(
		strings.ReplaceAll(strings.Join(queryFilters, " "), "-", "\\-"),
		".",
		"\\.",
	)
}

func (r *RedisPredictionStore) getSearchTagFilter(field, value string) string {
	return fmt.Sprintf("@%s:{%s}", field, value)
}

func (r *RedisPredictionStore) getSearchNumericRangeFilter(field string, from, to time.Time) string {
	return fmt.Sprintf("@%s:[%d %d]", field, from.UnixMilli(), to.UnixMilli())
}

func (r *RedisPredictionStore) parseResultsToPredictionsList(rawResult interface{}) ([]Prediction, error) {
	result, ok := rawResult.(map[interface{}]interface{})
	if !ok {
		return nil, ErrParsingListResult
	}

	rawResults, ok := result["results"]
	if !ok {
		return nil, ErrParsingListResult
	}

	results, ok := rawResults.([]interface{})
	if !ok {
		return nil, ErrParsingListResult
	}

	predictions := make([]Prediction, 0, len(results))

	for _, res := range results {
		prediction, err := r.parseResultToPrediction(res)
		if err != nil {
			return nil, err
		}

		predictions = append(predictions, *prediction)
	}

	return predictions, nil
}

func (r *RedisPredictionStore) parseResultToPrediction(res interface{}) (*Prediction, error) {
	rawResContent, ok := res.(map[interface{}]interface{})
	if !ok {
		return nil, ErrParsingListResult
	}

	rawExtraAttributes, ok := rawResContent["extra_attributes"]
	if !ok {
		return nil, ErrParsingListResult
	}

	extraAttributes, ok := rawExtraAttributes.(map[interface{}]interface{})
	if !ok {
		return nil, ErrParsingListResult
	}

	resJSONRoot, ok := extraAttributes["$"]
	if !ok {
		return nil, ErrParsingListResult
	}

	resJSONRootString, ok := resJSONRoot.(string)
	if !ok {
		return nil, ErrParsingListResult
	}

	var prediction *Prediction

	err := json.Unmarshal([]byte(resJSONRootString), &prediction)
	if err != nil {
		return nil, fmt.Errorf("unmarshalling prediction: %w", err)
	}

	return prediction, nil
}
