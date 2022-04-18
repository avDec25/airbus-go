package util

import (
	"bitbucket.mynt.myntra.com/plt/airbus-go/cache"
	"bitbucket.mynt.myntra.com/plt/airbus-go/constants"
	"bitbucket.mynt.myntra.com/plt/airbus-go/entry"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/satori/go.uuid"
	"strconv"
	"strings"
	"time"
	"os"
)
var SDK_VERSION="4.0.0"
var dcConfig = make(map[string]string)

func GetProducerConfig(serviceUrl string) (map[string]interface{}, error) {
	query,err:=getRequestMetaParams()
	if err!=nil{
		return nil, err
	}
	return getConfig(serviceUrl, constants.ProducerConfigPrefix+query)
}

func GetConsumerConfig(serviceUrl string) (map[string]interface{}, error) {
	query,err:=getRequestMetaParams()
	if err!=nil{
		return nil, err
	}
	return getConfig(serviceUrl, constants.ConsumerConfigPrefix+query)
}

func GetStatsdConfig(serviceUrl string) (map[string]interface{}, error) {
	return getConfig(serviceUrl, constants.StatsdConfigPrefix)
}

func GetDCConfig(serviceUrl string) (map[string]interface{}, error) {
	return getConfig(serviceUrl, constants.DCAppenderConfigPrefixPath)
}

func GetHosts(serviceUrl string) (map[string]interface{}, error) {
	return getConfig(serviceUrl, constants.DCAppenderConfigPrefixPath)
}

func GetConcurrencyLimit(serviceUrl string) (int, error) {
	response, err := getConfig(serviceUrl, constants.ConcurrencyLimitPath)
	if err != nil {
		return constants.DefautltConcurrencyLimit, err
	}
	if val, exists := response[constants.ConcurrencyPrefix]; !exists {
		return constants.DefautltConcurrencyLimit, errors.New("response don't have concurrency limit")
	} else {
		limit := fmt.Sprintf("%v", val)
		return strconv.Atoi(limit)
	}
}

func GetDCPrefix(serviceUrl string) (string, error) {
	prefix, ok := dcConfig[serviceUrl]
	if !ok {
		response, err := GetDCConfig(serviceUrl)
		if err != nil {
			return prefix, err
		}
		val, ok := response[constants.DCConfigPrefix]
		if !ok {
			return "", errors.New("empty response from service")
		} else {
			dcConfig[serviceUrl] = fmt.Sprintf("%v", val)
			prefix = fmt.Sprintf("%v", val)
		}
	}
	return prefix, nil
}

func GetClusterDetails(serviceUrl string, clusterId int)(map[string]interface{}, error){
	headers := make(map[string]string)
	headers["Accept"] = "application/json"
	headers["Content-Type"] = "application/json"
	headers["Authorization"] = constants.Authorization

	response, err := HttpGet(fmt.Sprintf("%s%s%s", serviceUrl, "/bus/v2/cluster/id/", strconv.Itoa(clusterId)), headers)
	if err != nil {
		log.Error(err.Error())
		return nil, errors.New(constants.ErrorFetchingClusterEntry)
	}
	data := response.(map[string]interface{})
	if statusCode := data["status"].(map[string]interface{})["statusCode"].(float64); statusCode == 3 {
		clusterdetails := data["clusterEntries"].([]interface{})
		
		if clusterdetails != nil && len(clusterdetails) > 0  {
			return clusterdetails[0].(map[string]interface{}), nil
		}
	}
	return nil, errors.New(constants.ErrorFetchingClusterEntry)
}	

func GetBootstrapServers(serviceUrl string, clusterId int)(string, error){
	clusterDetails, err := GetClusterDetails(serviceUrl, clusterId)
	if err!=nil{
		return "", err
	}
	var bootstrapServers map[string]string
	if byt, err := json.Marshal(clusterDetails["bootstrapServers"]); err == nil {
		
		if err = json.Unmarshal(byt, &bootstrapServers); err != nil {
			return "", err
		}
	}
	if err != nil {
		return "", err
	}

	dc, err := GetDCPrefix(serviceUrl)
	if err!=nil{
		return "", err
	}
	val, exists:= bootstrapServers[dc]
	if !exists{
		return "", errors.New("cannot find dc:"+dc)
	}
	return val,nil
}
		
func IsRecoverableClient(serviceUrl string) (bool, error) {
	response, err := getConfig(serviceUrl, constants.DCRecoveryConfigPath)
	if err != nil {
		return false, err
	}
	val, ok := response["state"]
	if !ok {
		return false, errors.New("empty response from service")
	}
	return strconv.ParseBool(fmt.Sprintf("%v", val))
}

func GetConsumerOffsetConfig(serviceUrl string, consumerGroupName string) ([]entry.OffsetEntry, error) {
	var offsetEntries []entry.OffsetEntry
	headers := make(map[string]string)
	headers["Accept"] = "application/json"
	headers["Content-Type"] = "application/json"
	headers["Authorization"] = constants.Authorization

	response, err := HttpGet(fmt.Sprintf("%s%s%s", serviceUrl, constants.ConsumerOffsetConfigPrefixPath, consumerGroupName), headers)
	if err != nil {
		log.Error(err.Error())
		return nil, errors.New(constants.ErrorFetchingOffsetEntry)
	}
	data := response.(map[string]interface{})
	if statusCode := data["status"].(map[string]interface{})["statusCode"].(float64); statusCode == 3 {
		offsetEntryUnmarshal := data["data"].([]interface{})
		if offsetEntryUnmarshal != nil && len(offsetEntryUnmarshal) > 0 && offsetEntryUnmarshal[0] != nil {
			for _, value := range offsetEntryUnmarshal {
				offsetEntry := entry.NewOffsetEntry(value.(map[string]interface{}))
				offsetEntries = append(offsetEntries, *offsetEntry)
			}
			return offsetEntries, nil
		}
	}
	return nil, errors.New(constants.ErrorOffsetConfigNotFound)
}

func getConfig(serviceUrl, configPrefix string) (map[string]interface{}, error) {
	headers := make(map[string]string)
	headers["Accept"] = "application/json"
	headers["Content-Type"] = "application/json"
	headers["Authorization"] = constants.Authorization

	response, err := HttpGet(serviceUrl+configPrefix, headers)
	if err != nil {
		log.Error(err.Error())
		return nil, errors.New(constants.ErrorFetchingSchema)
	}

	if response == nil {
		return nil, errors.New(constants.ErrorFetchingConfig)
	}

	return response.(map[string]interface{}), nil
}

func GetTopicName(serviceUrl, appName, eventName string) (string, error) {
	topicWithoutPrefix := fmt.Sprintf("%s-%s", appName, eventName)
	if appName == "" || eventName == "" {
		return "", errors.New(constants.AppEventNameNotFound)
	}
	if serviceUrl == "" {
		return topicWithoutPrefix, nil
	} else {
		if prefix, err := GetDCPrefix(serviceUrl); err == nil {
			topic := prefix + "." + appName + "-" + eventName
			if prefix == "" {
				return topicWithoutPrefix, nil
			}
			return topic, nil
		} else {
			return "", err
		}
	}
}

func GetAllDCTopicName(serviceUrl, appName, eventName string) ([]string, error) {
	var topicList []string
	if appName == "" || eventName == "" || serviceUrl == "" {
		return topicList, errors.New(constants.AppEventNameNotFound)
	}
	dcListConfig, err := getConfig(serviceUrl, constants.DCListConfigPath)
	if err == nil {
		if value, ok := dcListConfig["dc"].([]interface{}); !ok {
			err = errors.New("empty dc list config found in service")
		} else {
			for _, element := range value {
				if prefix := element.(string); prefix != "" {
					topic := prefix + "." + appName + "-" + eventName
					topicList = append(topicList, topic)
				} else {
					topic := appName + "-" + eventName
					topicList = append(topicList, topic)
				}
			}
		}
	}
	return topicList, err
}

func GetMetadataConsumerConfig() map[string]interface{} {
	metadataConsumerProps := make(map[string]interface{})
	uuidString := uuid.NewV4()
	metadataConsumerProps[constants.ConsumerGroup] = uuidString.String()
	return metadataConsumerProps
}

func GetConsumerGroupName(appName string, event *entry.EventEntry) (string, error) {
	if appName == "" || event.AppName == "" || event.EventName == "" {
		return "", errors.New(constants.AppEventNameNotFound)
	}
	return fmt.Sprintf("%s-%s-%s-%s", appName, event.AppName, event.EventName, constants.ConsumerGroupAppender), nil
}

func getTopicDetails(serviceUrl, topicName string) (*entry.TopicEntry, error) {
	arr := strings.Split(topicName, "-")
	headers := make(map[string]string)
	headers["Accept"] = "application/json"
	headers["Content-Type"] = "application/json"
	headers["Authorization"] = constants.Authorization

	response, err := HttpGet(fmt.Sprintf("%s%s%s/%s", serviceUrl, constants.GetTopicPrefix, arr[0], arr[1]), headers)
	if err != nil {
		log.Error(err.Error())
		return nil, errors.New(fmt.Sprintf(constants.ErrorFetchingTopic, topicName))
	}

	data := response.(map[string]interface{})
	if statusCode := data["status"].(map[string]interface{})["statusCode"].(float64); statusCode == 1002 {
		topicEntries := data["topicEntries"].([]interface{})
		if len(topicEntries) > 0 {
			return entry.NewTopicEntry(topicEntries[0].(map[string]interface{})), nil
		}
	}
	return nil, errors.New(fmt.Sprintf(constants.ErrorTopicNotFound, topicName))
}
func GetTopicDetails(serviceUrl, topicName string) (topicEntry *entry.TopicEntry, err error) {
	return getTopicDetails(serviceUrl, topicName)
}

func GetTopicDetailsCache(serviceUrl, topicName string) (topicEntry *entry.TopicEntry, err error) {
	if cacheData, found := cache.MemCache.Get("T:" + topicName); found {
		topicEntry = cacheData.(*entry.TopicEntry)
	} else {
		cache.MemCache.DeleteExpired()

		topicEntry, err = getTopicDetails(serviceUrl, topicName)
		if err == nil {
			cache.MemCache.Set("T:"+topicName, topicEntry, 6*time.Hour)
		}
	}
	return
}

func GetConsumerEntity(serviceUrl, appName string) ([]entry.AppEntry, error) {
	headers := make(map[string]string)
	headers["Accept"] = "application/json"
	headers["Content-Type"] = "application/json"
	headers["Authorization"] = constants.Authorization

	response, err := HttpGet(fmt.Sprintf("%s%s%s", serviceUrl, constants.ConsumerEntityPrefix, appName), headers)
	if err != nil {
		log.Error(err.Error())
		return nil, errors.New(constants.ErrorFetchingConsumerEntity)
	}

	data := response.(map[string]interface{})
	if statusCode := data["status"].(map[string]interface{})["statusCode"].(float64); statusCode == 1021 {
		appEntries, byt := []entry.AppEntry{}, []byte{}
		if byt, err = json.Marshal(data["data"]); err == nil {
			if err = json.Unmarshal(byt, &appEntries); err == nil {
				return appEntries, nil
			}
		}
		if err != nil {
			return nil, err
		}
	}
	return nil, errors.New(constants.ErrorAppNameNotFound)
}


func IsOffsetInMigration(serviceUrl, consumerGroup string) bool {
	headers := make(map[string]string)
	headers["Accept"] = "application/json"
	headers["Content-Type"] = "application/json"
	headers["Authorization"] = constants.Authorization

	response, err := HttpGet(fmt.Sprintf("%s%s%s", serviceUrl, "/bus/v2/offset/" , consumerGroup), headers)
	if err != nil {
		log.Error(err.Error())
		return false
	}

	data := response.(map[string]interface{})
	if statusCode := data["status"].(map[string]interface{})["statusCode"].(float64); statusCode == 3 {
		clusterdetails := data["data"].([]interface{})
		
		if clusterdetails != nil && len(clusterdetails) > 0  {
			return clusterdetails[0].(map[string]bool)["isInMigration"]
		}
	}
	return false
}
func getRequestMetaParams() (string, error){
	hostname, err := os.Hostname()
	if err!=nil{
		return "", err
	}
	query:="?hostname="+hostname+"&sdkversion="+SDK_VERSION
	return query,nil
}