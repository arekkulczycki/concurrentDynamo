package main

import (
    "context"
    "encoding/json"
    "log"
//     "fmt"
//     "reflect"
//     "math"
    "sync"
    "strings"
    "github.com/google/uuid"
    "github.com/aws/aws-lambda-go/lambda"
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/dynamodb"
    "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
    // "github.com/aws/aws-sdk-go/service/dynamodb/expression"

    // "strconv"
    // "strings"
    // "sync"

    // only for local
//     "os"
//     "io/ioutil"
)

// TODO::shared lib for go
type MyEvent struct {
    Body string `json:"body"`
}

type Response struct {
    IsBase64Encoded bool              `json:"isBase64Encoded"`
    StatusCode      int               `json:"statusCode"`
    Headers         map[string]string `json:"headers"`
    Body            string            `json:"body"`
}

func StructuredReturn(body string) (Response, error) {
    headers := make(map[string]string)
    headers["Access-Control-Allow-Origin"] = "*"
    response := Response{
        IsBase64Encoded: false,
        StatusCode:      200,
        Headers:         headers,
        Body:            body,
    }

    return response, nil
}

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}

// END TODO

func GetGuid(guid string) (string) {
    if guid == "" {
        randomUuid, _ := uuid.NewRandom()
        return randomUuid.String()
    }
    return guid
}

func PutItemDynamoDb(svc *dynamodb.DynamoDB, tableName string, item map[string]interface{}) (error) {
    itemDb, _ := dynamodbattribute.MarshalMap(item)

    input := &dynamodb.PutItemInput{
        Item: itemDb,
        TableName: aws.String(tableName),
    }

    svc.PutItem(input)

    return nil
}

func HandleUnityData(svc *dynamodb.DynamoDB, projectId string, unityData map[string]interface{}) (map[string]interface{}) {
    decodedUnityData := make(map[string]interface{})
    json.Unmarshal([]byte(unityData["unityData"].(string)), &decodedUnityData)

    towers := decodedUnityData["towers"].([]interface{})
    delete(decodedUnityData, "towers")

    unityDataTableName := "KyotopiaProjectsUnityData"

    var wg sync.WaitGroup

    for index, t := range towers {
        tower := t.(map[string]interface{})

        heliostats := tower["heliostats"].([]interface{})
        delete(tower, "heliostats")

        towerGuid := GetGuid(tower["guid"].(string))

        towerItem := make(map[string]interface{})
        towerItem["PK"] = projectId
        towerItem["SK"] = strings.Join([]string{"TOWER", towerGuid}, "#")
        towerItem["UnityData"] = tower

        maxBatchSize := 25

        for i := 0; i < len(heliostats); i += maxBatchSize {

            batch := heliostats[i:min(i+maxBatchSize, len(heliostats))]

            wg.Add(1)
            sem := make(chan struct{}, 20)
            go func(batch []interface{}, towerGuid string, index int){
                defer wg.Done()
                sem <- struct{}{}
                requestItems := make(map[string][]*dynamodb.WriteRequest)

                for _, h := range batch {
                    heliostat := h.(map[string]interface{})
                    heliostatGuid := GetGuid(heliostat["guid"].(string))
                    heliostatItem := make(map[string]interface{})
                    heliostatItem["PK"] = projectId
                    heliostatItem["SK"] = strings.Join([]string{"HELIOSTAT", towerGuid, heliostatGuid}, "#")
                    heliostatItem["UnityData"] = heliostat

                    heliostatItemDb, _ := dynamodbattribute.MarshalMap(heliostatItem)

                    writeRequest := &dynamodb.WriteRequest {
                        PutRequest: &dynamodb.PutRequest{
                            Item: heliostatItemDb,
                        },
                    }
                    requestItems[unityDataTableName] =  append(requestItems[unityDataTableName], writeRequest)
                }

                batchWriteItemInput := &dynamodb.BatchWriteItemInput {
                    RequestItems: requestItems,
                }

                batchWriteItemOutput, err := svc.BatchWriteItem(batchWriteItemInput)
                if err != nil {
                    batchWriteItemOutput, err2 := svc.BatchWriteItem(batchWriteItemInput)
                    if err2 != nil {
                        batchWriteItemOutput, err3 := svc.BatchWriteItem(batchWriteItemInput)
                        if err3 != nil {
                          log.Println("Error: ", err)
                        }
                    }
                }

                log.Println("Tower: ", index, " UnprocessedItems: ",  len(batchWriteItemOutput.UnprocessedItems[unityDataTableName]))

                <-sem
            }(batch, towerGuid, index)

        }
        wg.Wait()

//         wg.Add(len(heliostats))
//         for _, h := range heliostats {
//             heliostat := h.(map[string]interface{})
//
//             heliostatGuid := GetGuid(heliostat["guid"].(string))
//
//             heliostatItem := make(map[string]interface{})
//             heliostatItem["PK"] = projectId
//             heliostatItem["SK"] = strings.Join([]string{"HELIOSTAT", towerGuid, heliostatGuid}, "#")
//             heliostatItem["UnityData"] = heliostat
//
//             go func() {
//                 defer wg.Done()
//                 PutItemDynamoDb(svc, unityDataTableName, heliostatItem)
//             }()
//         }

        go func(towerItem map[string]interface{}) {
            defer wg.Done()
            PutItemDynamoDb(svc, unityDataTableName, towerItem)
        }(towerItem)
    }
    return decodedUnityData
}

func HandleImage(wg sync.WaitGroup, svc *dynamodb.DynamoDB, projectId string, image interface{}) {
    imageItem := make(map[string]interface{})
    imageItem["type"] = "design"
    imageItem["project_id"] = projectId
    imageItem["image"] = image

    imageTableName := "KyotopiaProjectsImage"

    wg.Add(1)
    go func() {
        defer wg.Done()
        PutItemDynamoDb(svc, imageTableName, imageItem)
    }()
}


func HandleRequest(ctx context.Context, event MyEvent) (Response, error) {
    log.Println("Handle request")
    var wg sync.WaitGroup

    sess := session.Must(session.NewSessionWithOptions(session.Options{
        SharedConfigState: session.SharedConfigEnable,
    }))
    svc := dynamodb.New(sess)

    projectsTableName := "KyotopiaProjects"

    data := make(map[string]interface{})
    json.Unmarshal([]byte(event.Body), &data)

    data["type"] = "design"
    projectId := data["id"]
    unityData := data["unityData"]

    delete(data, "unityData")
    unityDataJson, _ := json.Marshal(HandleUnityData(svc, projectId.(string), unityData.(map[string]interface{})))
    data["unityData"] = string(unityDataJson)

    settings := data["settings"]
    HandleImage(wg, svc, projectId.(string), settings.(map[string]interface{})["image"])

    wg.Add(1)
    go func() {
        defer wg.Done()
        PutItemDynamoDb(svc, projectsTableName, data)
    }()

    wg.Wait()

    return StructuredReturn(string("TEST"))
}

func main() {
//     argsWithoutProg := os.Args[1:]
//     if len(argsWithoutProg) > 0 && argsWithoutProg[0] == "local" {
//         log.Println("[Local] Calling with static params")
//         content, err := ioutil.ReadFile("saveProjectRequestPayload.json");
//         if err != nil {
//             log.Fatal(err)
//         }
//         event := MyEvent{}
//         eventStr := `{"body": ` + string(content) + `}`
//         json.Unmarshal([]byte(eventStr), &event)
//         HandleRequest(nil, event)
//     } else {
    lambda.Start(HandleRequest)
//     }
}
