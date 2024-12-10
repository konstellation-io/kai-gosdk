package metadata

import (
	"github.com/konstellation-io/kai-sdk/go-sdk/v1/internal/common"
	"github.com/spf13/viper"
)

type Metadata struct {
}

func New() *Metadata {
	return &Metadata{}
}

func (md Metadata) GetProduct() string {
	return viper.GetString(common.ConfigMetadataProductIDKey)
}

func (md Metadata) GetWorkflow() string {
	return viper.GetString(common.ConfigMetadataWorkflowIDKey)
}

func (md Metadata) GetWorkflowType() string {
	return viper.GetString(common.ConfigMetadataWorkflowTypeKey)
}

func (md Metadata) GetProcess() string {
	return viper.GetString(common.ConfigMetadataProcessIDKey)
}

func (md Metadata) GetProcessType() string {
	return viper.GetString(common.ConfigMetadataProcessTypeKey)
}

func (md Metadata) GetVersion() string {
	return viper.GetString(common.ConfigMetadataVersionIDKey)
}

func (md Metadata) GetEphemeralStorageName() string {
	return viper.GetString(common.ConfigNatsEphemeralStorage)
}

func (md Metadata) GetGlobalCentralizedConfigurationName() string {
	return viper.GetString(common.ConfigCcGlobalBucketKey)
}

func (md Metadata) GetProductCentralizedConfigurationName() string {
	return viper.GetString(common.ConfigCcProductBucketKey)
}

func (md Metadata) GetWorkflowCentralizedConfigurationName() string {
	return viper.GetString(common.ConfigCcWorkflowBucketKey)
}

func (md Metadata) GetProcessCentralizedConfigurationName() string {
	return viper.GetString(common.ConfigCcProcessBucketKey)
}
