// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package provider

import (
	"fmt"
	"os"
	"regexp"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

func TestAccIcebergNamespaceDataSource_Full(t *testing.T) {
	catalogURI := os.Getenv("ICEBERG_CATALOG_URI")
	if catalogURI == "" {
		t.Skip("ICEBERG_CATALOG_URI not set, skipping namespace data source E2E test")
	}

	providerCfg := fmt.Sprintf(providerConfig, catalogURI)

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: testAccIcebergNamespaceDataSourceBasicConfig(providerCfg, "namespace ds description"),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("data.iceberg_namespace.read", "name.0", "ns_ds_basic"),
					resource.TestCheckResourceAttr("data.iceberg_namespace.read", "id", "ns_ds_basic"),
					resource.TestCheckResourceAttrPair("data.iceberg_namespace.read", "id", "iceberg_namespace.subject", "id"),
					resource.TestCheckResourceAttr("data.iceberg_namespace.read", "server_properties.description", "namespace ds description"),
				),
			},
			{
				Config: testAccIcebergNamespaceDataSourceNestedConfig(providerCfg),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("data.iceberg_namespace.read", "name.0", "analytics"),
					resource.TestCheckResourceAttr("data.iceberg_namespace.read", "name.1", "prod"),
					resource.TestCheckResourceAttr("data.iceberg_namespace.read", "id", "analytics.prod"),
					resource.TestCheckResourceAttrPair("data.iceberg_namespace.read", "id", "iceberg_namespace.subject", "id"),
					resource.TestCheckResourceAttr("data.iceberg_namespace.read", "server_properties.owner", "data-team"),
				),
			},
		},
	})
}

func TestAccIcebergNamespaceDataSource_NotFound(t *testing.T) {
	catalogURI := os.Getenv("ICEBERG_CATALOG_URI")
	if catalogURI == "" {
		t.Skip("ICEBERG_CATALOG_URI not set, skipping namespace data source E2E test")
	}

	providerCfg := fmt.Sprintf(providerConfig, catalogURI)

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config:      testAccIcebergNamespaceDataSourceMissingConfig(providerCfg),
				ExpectError: regexp.MustCompile(`Namespace not found`),
			},
		},
	})
}

func testAccIcebergNamespaceDataSourceBasicConfig(providerCfg string, description string) string {
	return providerCfg + fmt.Sprintf(`
resource "iceberg_namespace" "subject" {
  name = ["ns_ds_basic"]
  user_properties = {
    description = "%s"
  }
}

data "iceberg_namespace" "read" {
  name = iceberg_namespace.subject.name
}
`, description)
}

func testAccIcebergNamespaceDataSourceNestedConfig(providerCfg string) string {
	return providerCfg + `
resource "iceberg_namespace" "subject" {
  name = ["analytics", "prod"]
  user_properties = {
    owner = "data-team"
  }
}

data "iceberg_namespace" "read" {
  name = iceberg_namespace.subject.name
}
`
}

func testAccIcebergNamespaceDataSourceMissingConfig(providerCfg string) string {
	return providerCfg + `
data "iceberg_namespace" "missing" {
  name = ["definitely_no_such_namespace"]
}
`
}
