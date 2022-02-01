package julie

import (
	"context"
	"fmt"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	"log"
	client "terraform-provider-julieops/julie/client"
	"testing"
)

var testProvider *schema.Provider

var testAccProviders = map[string]*schema.Provider{
	"julieops": func() *schema.Provider {
		provider, _ := overrideProvider()
		return provider
	}(),
}
var testBootstrapServers string = bootstrapServersFromEnv()

func TestProvider(t *testing.T) {
	if err := Provider().InternalValidate(); err != nil {
		t.Fatalf("err: %s", err)
	}
}

func testAccPreCheck(t *testing.T) {
	log.Printf("testAccPreCheck %t", testProvider == nil)
	meta := testProvider.Meta()
	if meta == nil {
		t.Fatal("Could not construct client")
	}
	client := meta.(*client.KafkaCluster)
	if client == nil {
		t.Fatal("No client")
	}
}

func overrideProviderFactory() map[string]func() (*schema.Provider, error) {
	log.Printf("overrideProviderFactory")
	return map[string]func() (*schema.Provider, error){
		"julieops": func() (*schema.Provider, error) {
			return overrideProvider()
		},
	}
}

func overrideProvider() (*schema.Provider, error) {
	log.Println("[INFO] Setting up override for a provider")
	provider := Provider()

	rc, err := accTestProviderConfig()
	if err != nil {
		return nil, err
	}
	diags := provider.Configure(context.Background(), rc)
	if diags.HasError() {
		log.Printf("[ERROR] Could not configure provider %v", diags)
		return nil, fmt.Errorf("Could not configure provider")
	}

	testProvider = provider
	return provider, nil
}

func accTestProviderConfig() (*terraform.ResourceConfig, error) {
	raw := map[string]interface{}{
		"bootstrap_servers": bootstrapServersFromEnv(),
	}
	return terraform.NewResourceConfigRaw(raw), nil
}

func bootstrapServersFromEnv() string {
	return "localhost:29092"
}
