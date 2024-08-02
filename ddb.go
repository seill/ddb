package ddb

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/seill/util"
)

const (
	GSI1 = "GSI1"
	GSI2 = "GSI2"
	GSI3 = "GSI3"
	GSI4 = "GSI4"
	GSI5 = "GSI5"
)

type IDynamoDbRecord interface {
	BuildPk(id string) (pk string)
}

type DynamoDbMetaData struct {
	IDynamoDbRecord  `json:"-" dynamodbav:"-"`
	Id               string     `json:",omitempty" dynamodbav:",omitempty"`
	PK               string     `json:"-" dynamodbav:",omitempty"`
	SK               string     `json:"-" dynamodbav:",omitempty"`
	GSI1PK           *string    `json:"-" dynamodbav:",omitempty"`
	GSI1SK           *string    `json:"-" dynamodbav:",omitempty"`
	GSI2PK           *string    `json:"-" dynamodbav:",omitempty"`
	GSI2SK           *string    `json:"-" dynamodbav:",omitempty"`
	GSI3PK           *string    `json:"-" dynamodbav:",omitempty"`
	GSI3SK           *string    `json:"-" dynamodbav:",omitempty"`
	GSI4PK           *string    `json:"-" dynamodbav:",omitempty"`
	GSI4SK           *string    `json:"-" dynamodbav:",omitempty"`
	GSI5PK           *string    `json:"-" dynamodbav:",omitempty"`
	GSI5SK           *string    `json:"-" dynamodbav:",omitempty"`
	CreatedTimestamp *time.Time `json:",omitempty" dynamodbav:",omitempty"`
	UpdatedTimestamp *time.Time `json:",omitempty" dynamodbav:",omitempty"`
}

func Marshal(m IDynamoDbRecord) (data []byte, err error) {
	data, err = json.Marshal(m)
	return
}

func Unmarshal(data []byte, m IDynamoDbRecord) (err error) {
	err = json.Unmarshal(data, &m)
	return
}

type QueryOptionOrder struct {
	Field     string `json:"field"`
	Direction string `json:"direction"`
}

type QueryOptionPage struct {
	PageSize         uint64      `json:"pageSize" validate:"required"`
	LastEvaluatedKey interface{} `json:"lastEvaluatedKey" validate:"required" default:"{}"`
}

type QueryOption struct {
	Filter           map[string]interface{} `json:"filter"`
	Order            []QueryOptionOrder     `json:"order"`
	ScanIndexForward *bool                  `json:"scanIndexForward"`
	Page             *QueryOptionPage       `json:"page" validate:"required"`
}

type Ddb struct {
	dynamoDb  *dynamodb.Client
	tableName string
}

func New(dynamoDb *dynamodb.Client, tableName string) *Ddb {
	return &Ddb{
		dynamoDb:  dynamoDb,
		tableName: tableName,
	}
}

type Key struct {
	PK        *string `json:",omitempty" dynamodbav:",omitempty"`
	SK        *string `json:",omitempty" dynamodbav:",omitempty"`
	IndexName *string `json:",omitempty" dynamodbav:",omitempty"`
	Condition *string `json:",omitempty" dynamodbav:",omitempty"`
}

func (r *Ddb) GetItem(key Key) (item map[string]types.AttributeValue, err error) {
	//log.DebugJson("repository::GetItem", "key", key)

	if nil == key.IndexName {
		var av map[string]types.AttributeValue
		var output *dynamodb.GetItemOutput

		av, err = attributevalue.MarshalMap(key)
		if err != nil {
			return
		}

		input := &dynamodb.GetItemInput{
			Key:       av,
			TableName: aws.String(r.tableName),
		}

		//log.DebugJson("repository::GetItem", "input", input)

		output, err = r.dynamoDb.GetItem(context.TODO(), input)
		if err != nil {
			return
		}

		if nil == output.Item {
			err = fmt.Errorf("item not found (%s)", util.StructToString(key))
			return
		}

		//log.DebugJson("repository::getItemViaGsi", "output", output)

		item = output.Item
	} else {
		item, err = r.getItemViaGsi(key)
	}

	//log.DebugJson("repository::GetItem", "item", item)

	return
}

func (r *Ddb) getItemViaGsi(key Key) (item map[string]types.AttributeValue, err error) {
	var expressionAttributeValues map[string]types.AttributeValue
	var keyConditionExpression string

	expressionAttributeValues = map[string]types.AttributeValue{
		":gsipk": &types.AttributeValueMemberS{Value: *key.PK},
	}
	keyConditionExpression = fmt.Sprintf("%sPK = :gsipk", *key.IndexName)

	if nil != key.SK {
		expressionAttributeValues[":gsisk"] = &types.AttributeValueMemberS{Value: *key.SK}
		if strings.HasSuffix(*key.SK, "#") {
			keyConditionExpression = fmt.Sprintf("%sPK = :gsipk and begins_with(%sSK, :gsisk)", *key.IndexName, *key.IndexName)
		} else {
			keyConditionExpression = fmt.Sprintf("%sPK = :gsipk and %sSK = :gsisk", *key.IndexName, *key.IndexName)
		}
	}
	if nil != key.Condition {
		expressionAttributeValues[":gsisk"] = &types.AttributeValueMemberS{Value: *key.SK}
		switch *key.Condition {
		case "equal":
			keyConditionExpression = fmt.Sprintf("%sPK = :gsipk and %sSK = :gsisk", *key.IndexName, *key.IndexName)
		case "more_than":
			keyConditionExpression = fmt.Sprintf("%sPK = :gsipk and %sSK > :gsisk", *key.IndexName, *key.IndexName)
		case "less_than":
			keyConditionExpression = fmt.Sprintf("%sPK = :gsipk and %sSK < :gsisk", *key.IndexName, *key.IndexName)
		case "more_than_equal":
			keyConditionExpression = fmt.Sprintf("%sPK = :gsipk and %sSK >= :gsisk", *key.IndexName, *key.IndexName)
		case "less_than_equal":
			keyConditionExpression = fmt.Sprintf("%sPK = :gsipk and %sSK <= :gsisk", *key.IndexName, *key.IndexName)
		case "begins":
			keyConditionExpression = fmt.Sprintf("%sPK = :gsipk and begins_with(%sSK, :gsisk)", *key.IndexName, *key.IndexName)
		}
	}

	input := &dynamodb.QueryInput{
		TableName:                 aws.String(r.tableName),
		IndexName:                 key.IndexName,
		KeyConditionExpression:    aws.String(keyConditionExpression),
		ExpressionAttributeValues: expressionAttributeValues,
		Limit:                     aws.Int32(1),
	}

	//log.DebugJson("repository::getItemViaGsi", "input", input)

	output, err := r.dynamoDb.Query(context.TODO(), input)
	if err != nil {
		return
	}

	//log.DebugJson("repository::getItemViaGsi", "output", output)

	if 0 == len(output.Items) {
		err = fmt.Errorf("item not found (%s)", util.StructToString(key))
		return
	}

	item = output.Items[0]

	return
}

func (r *Ddb) DeleteItem(key Key) (err error) {
	//log.DebugJson("repository::DeleteItem", "key", key)

	var av map[string]types.AttributeValue
	//var output *dynamodb.DeleteItemOutput

	av, err = attributevalue.MarshalMap(key)
	if err != nil {
		return
	}

	input := &dynamodb.DeleteItemInput{
		Key:       av,
		TableName: aws.String(r.tableName),
	}

	//log.DebugJson("repository::DeleteItem", "input", input)

	_, err = r.dynamoDb.DeleteItem(context.TODO(), input)

	//log.DebugJson("repository::DeleteItem", "output", output)

	return
}

func (r *Ddb) CreateItem(item interface{}) (err error) {
	avItem, err := attributevalue.MarshalMap(item)
	if err != nil {
		return
	}

	input := &dynamodb.PutItemInput{
		Item:      avItem,
		TableName: aws.String(r.tableName),
	}

	//log.DebugJson("repository", "Insert", input)

	_, err = r.dynamoDb.PutItem(context.TODO(), input)
	if err != nil {
		return
	}

	return
}

func (r *Ddb) UpdateItem(key Key, propertyMap map[string]interface{}) (output *dynamodb.UpdateItemOutput, err error) {
	var keyAv map[string]types.AttributeValue
	var expressionAv map[string]types.AttributeValue
	var expressionAttributeNames = map[string]string{}
	var expressionAttributeValues = map[string]interface{}{}
	var expressionNamesAndValues = map[string]string{}
	var updateExpressions []string

	keyAv, err = attributevalue.MarshalMap(key)
	if err != nil {
		return
	}

	// add UpdatedTimestamp
	propertyMap["UpdatedTimestamp"] = time.Now()

	err = buildExpressionAttributeNamesAndValue(nil, propertyMap, &expressionAttributeNames, &expressionAttributeValues, &expressionNamesAndValues)
	if nil != err {
		return
	}

	expressionAv, err = attributevalue.MarshalMap(expressionAttributeValues)
	if err != nil {
		return
	}

	for k, v := range expressionNamesAndValues {
		updateExpressions = append(updateExpressions, fmt.Sprintf("%s=%s", k, v))
	}

	//log.DebugJson("", strings.Join(updateExpressions, ", "), nil)
	input := &dynamodb.UpdateItemInput{
		Key:                       keyAv,
		TableName:                 aws.String(r.tableName),
		ExpressionAttributeNames:  expressionAttributeNames,
		ExpressionAttributeValues: expressionAv,
		UpdateExpression:          aws.String(fmt.Sprintf("set %s", strings.Join(updateExpressions, ", "))),
		ReturnValues:              types.ReturnValueUpdatedNew,
	}

	output, err = r.dynamoDb.UpdateItem(context.TODO(), input)

	return
}

func buildExpressionAttributeNamesAndValue(parentName *[]string, mapData map[string]interface{}, expressionAttributeNames *map[string]string, expressionAttributeValues *map[string]interface{}, expressionNamesAndValues *map[string]string) (err error) {
	for k, v := range mapData {
		var isFunction = 0 == strings.Index(k, "Fn:")
		var keysFunction []string

		if isFunction {
			keysFunction = strings.Split(k, ":") // Fn:function_name:key

			if 3 == len(keysFunction) {
				switch keysFunction[1] { // function_name
				case "list_append":
					k = keysFunction[2]
					//v = fmt.Sprintf("list_append(%s, %s)",
				case "increase", "decrease":
					k = keysFunction[2]
					(*expressionAttributeValues)[":_Zero"] = 0
				default:
					err = fmt.Errorf("unsupported function name (%s)", k)
					return
				}
			} else {
				err = fmt.Errorf("unsupported function format (%s)", k)
				return
			}
		}

		(*expressionAttributeNames)["#"+k] = k

		if !isFunction && reflect.ValueOf(v).Kind() == reflect.Map {
			var _parentName []string

			if nil == parentName {
				_parentName = []string{k}
			} else {
				if 1 <= len(*parentName) {
					goto build
				}

				_parentName = append(*parentName, k)
				//parentName = &_parentName
			}

			err = buildExpressionAttributeNamesAndValue(&_parentName, v.(map[string]interface{}), expressionAttributeNames, expressionAttributeValues, expressionNamesAndValues)
			if nil != err {
				return
			}

			continue
		}

	build:
		if nil == parentName {
			(*expressionAttributeValues)[fmt.Sprintf(":%s", k)] = v

			if isFunction {
				(*expressionNamesAndValues)[fmt.Sprintf("#%s", k)], err = buildFunctionForExpressionAttributeNamesAndValue(parentName, keysFunction[1], k)
				if nil != err {
					return
				}
			} else {
				(*expressionNamesAndValues)[fmt.Sprintf("#%s", k)] = fmt.Sprintf(":%s", k)
			}
		} else {
			(*expressionAttributeValues)[fmt.Sprintf(":%s", strings.Join(append(*parentName, k), "_"))] = v

			if isFunction {
				(*expressionNamesAndValues)[fmt.Sprintf("#%s", strings.Join(append(*parentName, k), ".#"))], err = buildFunctionForExpressionAttributeNamesAndValue(parentName, keysFunction[1], k)
				if nil != err {
					return
				}
			} else {
				(*expressionNamesAndValues)[fmt.Sprintf("#%s", strings.Join(append(*parentName, k), ".#"))] = fmt.Sprintf(":%s", strings.Join(append(*parentName, k), "_"))
			}
		}
	}

	return
}

func buildFunctionForExpressionAttributeNamesAndValue(parentName *[]string, functionName string, key string) (function string, err error) {
	var key1, key2 string

	if nil == parentName {
		key1 = key
		key2 = key
	} else {
		key1 = fmt.Sprintf("#%s.#%s", strings.Join(*parentName, ".#"), key)
		key2 = fmt.Sprintf("%s_%s", strings.Join(*parentName, "_"), key)
	}

	switch functionName {
	case "list_append":
		function = fmt.Sprintf("list_append(%s, :%s)", key1, key2)
	case "increase":
		function = fmt.Sprintf("if_not_exists(%s, :_Zero) + :%s", key1, key2)
	case "decrease":
		function = fmt.Sprintf("if_not_exists(%s, :_Zero) - :%s", key1, key2)
	default:
		err = fmt.Errorf("unsupported function name (%s)", functionName)
	}

	return
}

func (r *Ddb) GetListItem(key Key, arrayOfField string, queryOption QueryOption) (items []map[string]types.AttributeValue, lastEvaluatedKey interface{}, err error) {
	var output *dynamodb.QueryOutput
	var expressionAttributeValues map[string]types.AttributeValue
	var keyConditionExpression string
	var scanIndexForward = queryOption.ScanIndexForward

	if nil == scanIndexForward {
		scanIndexForward = aws.Bool(true)
	}

	expressionAttributeValues = make(map[string]types.AttributeValue)

	expressionAttributeValues[":gsipk"] = &types.AttributeValueMemberS{Value: *key.PK}

	keyConditionExpression = fmt.Sprintf("#%sPK = :gsipk", *key.IndexName)

	if nil != key.SK {
		if strings.Contains(*key.SK, "/") {
			skRange := strings.Split(*key.SK, "/")
			expressionAttributeValues[":from"] = &types.AttributeValueMemberS{Value: skRange[0]}
			expressionAttributeValues[":to"] = &types.AttributeValueMemberS{Value: skRange[1]}

			keyConditionExpression = fmt.Sprintf("#%sPK = :gsipk and #%sSK BETWEEN :from AND :to", *key.IndexName, *key.IndexName)
		} else {
			expressionAttributeValues[":gsisk"] = &types.AttributeValueMemberS{Value: *key.SK}
			if strings.HasSuffix(*key.SK, "#") {
				keyConditionExpression = fmt.Sprintf("#%sPK = :gsipk and begins_with(#%sSK, :gsisk)", *key.IndexName, *key.IndexName)
			} else {
				keyConditionExpression = fmt.Sprintf("#%sPK = :gsipk and #%sSK = :gsisk", *key.IndexName, *key.IndexName)
			}
		}
	}

	if nil != key.Condition {
		if strings.Contains(*key.SK, "/") {
			skRange := strings.Split(*key.SK, "/")
			expressionAttributeValues[":from"] = &types.AttributeValueMemberS{Value: skRange[0]}
			expressionAttributeValues[":to"] = &types.AttributeValueMemberS{Value: skRange[1]}

			switch *key.Condition {
			case "between":
				keyConditionExpression = fmt.Sprintf("#%sPK = :gsipk and #%sSK BETWEEN :from AND :to", *key.IndexName, *key.IndexName)
			}
		} else {
			expressionAttributeValues[":gsisk"] = &types.AttributeValueMemberS{Value: *key.SK}
			switch *key.Condition {
			case "equal":
				keyConditionExpression = fmt.Sprintf("#%sPK = :gsipk and #%sSK = :gsisk", *key.IndexName, *key.IndexName)
			case "more_than":
				keyConditionExpression = fmt.Sprintf("#%sPK = :gsipk and #%sSK > :gsisk", *key.IndexName, *key.IndexName)
			case "less_than":
				keyConditionExpression = fmt.Sprintf("#%sPK = :gsipk and #%sSK < :gsisk", *key.IndexName, *key.IndexName)
			case "more_than_equal":
				keyConditionExpression = fmt.Sprintf("#%sPK = :gsipk and #%sSK >= :gsisk", *key.IndexName, *key.IndexName)
			case "less_than_equal":
				keyConditionExpression = fmt.Sprintf("#%sPK = :gsipk and #%sSK <= :gsisk", *key.IndexName, *key.IndexName)
			case "begins":
				keyConditionExpression = fmt.Sprintf("#%sPK = :gsipk and begins_with(#%sSK, :gsisk)", *key.IndexName, *key.IndexName)
			}
		}
	}

	expressionAttributeNames := make(map[string]string)
	expressionAttributeNames[fmt.Sprintf("#%sPK", *key.IndexName)] = fmt.Sprintf("%sPK", *key.IndexName)
	expressionAttributeNames[fmt.Sprintf("#%sSK", *key.IndexName)] = fmt.Sprintf("%sSK", *key.IndexName)

	input := &dynamodb.QueryInput{
		ExpressionAttributeNames:  expressionAttributeNames,
		ExpressionAttributeValues: expressionAttributeValues,
		KeyConditionExpression:    aws.String(keyConditionExpression),
		TableName:                 aws.String(r.tableName),
		ScanIndexForward:          scanIndexForward,
	}

	if queryOption.Filter != nil {
		filterExpression, _ := processQueryOptionFilter(queryOption.Filter, expressionAttributeValues, expressionAttributeNames)
		input.FilterExpression = aws.String(filterExpression)
	}

	if queryOption.Page != nil && queryOption.Page.LastEvaluatedKey != nil {
		LastEvaluatedKey, err := attributevalue.MarshalMap(queryOption.Page.LastEvaluatedKey)
		if err != nil {
			return nil, nil, err
		}
		input.ExclusiveStartKey = LastEvaluatedKey
	}

	if queryOption.Page != nil && 0 < queryOption.Page.PageSize {
		input.Limit = aws.Int32(int32(queryOption.Page.PageSize))
	}

	if key.IndexName != nil {
		input.IndexName = key.IndexName
	}

	if arrayOfField != "" {
		temp := strings.Split(arrayOfField, ",")
		tempArrayOfField := make([]string, len(temp))
		for i, v := range temp {
			expressionAttributeNames[fmt.Sprintf("#%s", v)] = strings.ReplaceAll(v, "#", "")
			tempArrayOfField[i] = fmt.Sprintf("#%s", v)
		}
		input.ProjectionExpression = aws.String(strings.Join(tempArrayOfField, ","))
	}

	output, err = r.dynamoDb.Query(context.TODO(), input)
	if err != nil {
		return
	}
	if len(output.Items) < 1 && nil == output.LastEvaluatedKey {
		err = fmt.Errorf("item not found (%s)", util.StructToString(key))
		return
	}
	LastEvaluatedKey := new(map[string]interface{})
	if output.LastEvaluatedKey != nil {
		if err = attributevalue.UnmarshalMap(output.LastEvaluatedKey, &LastEvaluatedKey); err != nil {
			return
		}
		lastEvaluatedKey = LastEvaluatedKey
	}
	items = output.Items
	return
}

func processQueryOptionFilter(filters map[string]interface{}, expressionAttributeValues map[string]types.AttributeValue, expressionAttributeName map[string]string) (filterExpression string, err error) {
	if nil == filters {
		return
	}
	var i = 0

	//parent condition
	for _, value := range filters {
		item := value.(map[string]interface{})
		if item["position"] == nil {
			filter := getWhere(item)
			if i == 0 {
				filterExpression += filter
			} else {
				if item["condition"] != nil {
					filterExpression += " " + item["condition"].(string) + " " + filter
				} else {
					filterExpression += " AND " + filter
				}
			}
			expressionAttributeName["#"+item["field"].(string)] = item["field"].(string)

			if item["type"].(string) == "date" {
				date := strings.Split(item["keyword"].(string), "/")
				expressionAttributeValues[":"+item["field"].(string)+"Start"] = &types.AttributeValueMemberS{Value: date[0]}
				expressionAttributeValues[":"+item["field"].(string)+"End"] = &types.AttributeValueMemberS{Value: date[1]}
			} else if item["type"].(string) != "bool" {
				expressionAttributeValues[":"+item["field"].(string)] = &types.AttributeValueMemberS{Value: item["keyword"].(string)}
			} else {
				expressionAttributeValues[":"+item["field"].(string)] = &types.AttributeValueMemberBOOL{Value: item["keyword"].(bool)}
			}
			i++
		}
	}

	//child condition
	for _, value := range filters {
		item := value.(map[string]interface{})
		if item["position"] != nil {
			if expressionAttributeName["#"+item["field"].(string)] != "" {
				item["field"] = item["field"].(string) + "_2"
			}
			filter := getWhere(item)
			if i == 0 {
				filterExpression += filter
			} else {
				if item["condition"] != nil {
					filterExpression += " " + item["condition"].(string) + " " + filter
				} else {
					filterExpression += " AND " + filter
				}
			}
			expressionAttributeName["#"+item["field"].(string)] = strings.ReplaceAll(item["field"].(string), "_2", "")

			if item["type"].(string) == "date" {
				date := strings.Split(item["keyword"].(string), "/")
				expressionAttributeValues[":"+item["field"].(string)+"Start"] = &types.AttributeValueMemberS{Value: date[0]}
				expressionAttributeValues[":"+item["field"].(string)+"End"] = &types.AttributeValueMemberS{Value: date[1]}
			} else if item["type"].(string) != "bool" {
				expressionAttributeValues[":"+item["field"].(string)] = &types.AttributeValueMemberS{Value: item["keyword"].(string)}
			} else {
				expressionAttributeValues[":"+item["field"].(string)] = &types.AttributeValueMemberBOOL{Value: item["keyword"].(bool)}
			}
			i++
		}
	}

	return
}

func getWhere(item map[string]interface{}) (filterExpression string) {

	field := item["field"].(string)
	searchType := item["type"].(string)
	field = strings.ReplaceAll(field, "\"", "'")
	switch searchType {
	case "keyword":
		filterExpression = fmt.Sprintf("contains (#%s, :%s)", field, field)
	case "equal":
		filterExpression = fmt.Sprintf("#%s = :%s", field, field)
	case "bool":
		filterExpression = fmt.Sprintf("#%s = :%s", field, field)
	case "not_equal":
		filterExpression = fmt.Sprintf("#%s <> :%s", field, field)
	case "date":
		filterExpression = fmt.Sprintf("#%s BETWEEN :%s AND :%s", field, field+"Start", field+"End")
	case "less_than_equal":
		filterExpression = fmt.Sprintf("#%s <= :%s", field, field)
	case "less_than":
		filterExpression = fmt.Sprintf("#%s <= :%s", field, field)
	case "more_than_equal":
		filterExpression = fmt.Sprintf("#%s => :%s", field, field)
	case "more_than":
		filterExpression = fmt.Sprintf("#%s > :%s", field, field)
	}

	// log.Debug("getWhere %s %v", query, arg)

	return
}
