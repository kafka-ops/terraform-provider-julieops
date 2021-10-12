package julie

import (
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	client "terraform-provider-julieops/julie/client"
	"testing"
)

var testProvider *schema.Provider
var testBootstrapServers string = bootstrapServersFromEnv()


func TestProvider(t *testing.T) {
	if err := Provider().InternalValidate(); err != nil {
		t.Fatalf("err: %s", err)
	}
}

func testAccPreCheck(t *testing.T) {
	meta := testProvider.Meta()
	if meta == nil {
		t.Fatal("Could not construct client")
	}
	client := meta.(*client.KafkaCluster)
	if client == nil {
		t.Fatal("No client")
	}
}


func bootstrapServersFromEnv() string {
	return "localhost:29092"
}