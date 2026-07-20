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

	"github.com/apache/iceberg-go/table"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/stretchr/testify/assert"
)

func TestIdentifierString(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "db.events", identifierString(table.Identifier{"db", "events"}))
	assert.Equal(t, "analytics.raw.orders", identifierString(table.Identifier{"analytics", "raw", "orders"}))
	assert.Equal(t, "", identifierString(nil))
}

func TestTableNamesFromIdentifiers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ids  []table.Identifier
		want []string
	}{
		{
			name: "empty",
			ids:  nil,
			want: []string{},
		},
		{
			name: "single table in flat namespace",
			ids:  []table.Identifier{{"db", "events"}},
			want: []string{"events"},
		},
		{
			name: "multiple tables sorted alphabetically",
			ids: []table.Identifier{
				{"analytics", "raw", "orders"},
				{"analytics", "raw", "events"},
			},
			want: []string{"events", "orders"},
		},
		{
			name: "nested namespace",
			ids:  []table.Identifier{{"analytics", "prod", "metrics"}},
			want: []string{"metrics"},
		},
		{
			name: "stable sort preserves relative order for equal keys",
			ids: []table.Identifier{
				{"ns", "b"},
				{"ns", "a"},
				{"ns", "a"},
			},
			want: []string{"a", "a", "b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tableNamesFromIdentifiers(tt.ids))
		})
	}
}

func TestTableIdentifierStrings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ids  []table.Identifier
		want []string
	}{
		{
			name: "empty",
			ids:  nil,
			want: []string{},
		},
		{
			name: "single table in flat namespace",
			ids:  []table.Identifier{{"db", "events"}},
			want: []string{"db.events"},
		},
		{
			name: "multiple tables sorted alphabetically",
			ids: []table.Identifier{
				{"analytics", "raw", "orders"},
				{"analytics", "raw", "events"},
			},
			want: []string{"analytics.raw.events", "analytics.raw.orders"},
		},
		{
			name: "nested namespace",
			ids:  []table.Identifier{{"analytics", "prod", "metrics"}},
			want: []string{"analytics.prod.metrics"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tableIdentifierStrings(tt.ids))
		})
	}
}

func TestAccIcebergTablesDataSource_Full(t *testing.T) {
	catalogURI := os.Getenv("ICEBERG_CATALOG_URI")
	if catalogURI == "" {
		t.Skip("ICEBERG_CATALOG_URI not set, skipping tables data source E2E test")
	}

	providerCfg := fmt.Sprintf(providerConfig, catalogURI)

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: testAccIcebergTablesDataSourceBasicConfig(providerCfg),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("data.iceberg_tables.read", "namespace.0", "ns_tables_ds_basic"),
					resource.TestCheckResourceAttr("data.iceberg_tables.read", "id", "ns_tables_ds_basic"),
					resource.TestCheckResourceAttr("data.iceberg_tables.read", "tables.#", "2"),
					resource.TestCheckResourceAttr("data.iceberg_tables.read", "tables.0", "alpha_table"),
					resource.TestCheckResourceAttr("data.iceberg_tables.read", "tables.1", "beta_table"),
					resource.TestCheckResourceAttr("data.iceberg_tables.read", "identifiers.#", "2"),
					resource.TestCheckResourceAttr("data.iceberg_tables.read", "identifiers.0", "ns_tables_ds_basic.alpha_table"),
					resource.TestCheckResourceAttr("data.iceberg_tables.read", "identifiers.1", "ns_tables_ds_basic.beta_table"),
				),
			},
			{
				Config: testAccIcebergTablesDataSourceNestedConfig(providerCfg),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("data.iceberg_tables.read", "namespace.0", "analytics"),
					resource.TestCheckResourceAttr("data.iceberg_tables.read", "namespace.1", "raw"),
					resource.TestCheckResourceAttr("data.iceberg_tables.read", "id", "analytics.raw"),
					resource.TestCheckResourceAttr("data.iceberg_tables.read", "tables.#", "1"),
					resource.TestCheckResourceAttr("data.iceberg_tables.read", "tables.0", "events"),
					resource.TestCheckResourceAttr("data.iceberg_tables.read", "identifiers.#", "1"),
					resource.TestCheckResourceAttr("data.iceberg_tables.read", "identifiers.0", "analytics.raw.events"),
				),
			},
			{
				Config: testAccIcebergTablesDataSourceEmptyNamespaceConfig(providerCfg),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("data.iceberg_tables.read", "namespace.0", "ns_tables_ds_empty"),
					resource.TestCheckResourceAttr("data.iceberg_tables.read", "tables.#", "0"),
					resource.TestCheckResourceAttr("data.iceberg_tables.read", "identifiers.#", "0"),
				),
			},
		},
	})
}

func TestAccIcebergTablesDataSource_NotFound(t *testing.T) {
	catalogURI := os.Getenv("ICEBERG_CATALOG_URI")
	if catalogURI == "" {
		t.Skip("ICEBERG_CATALOG_URI not set, skipping tables data source E2E test")
	}

	providerCfg := fmt.Sprintf(providerConfig, catalogURI)

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config:      testAccIcebergTablesDataSourceMissingConfig(providerCfg),
				ExpectError: regexp.MustCompile(`Namespace not found`),
			},
		},
	})
}

func testAccIcebergTablesDataSourceBasicConfig(providerCfg string) string {
	return providerCfg + `
resource "iceberg_namespace" "db" {
  name = ["ns_tables_ds_basic"]
}

resource "iceberg_table" "alpha" {
  namespace = iceberg_namespace.db.name
  name      = "alpha_table"
  schema = {
    fields = [
      {
        id       = 1
        name     = "id"
        type     = "long"
        required = true
      }
    ]
  }
}

resource "iceberg_table" "beta" {
  namespace = iceberg_namespace.db.name
  name      = "beta_table"
  schema = {
    fields = [
      {
        id       = 1
        name     = "id"
        type     = "long"
        required = true
      }
    ]
  }
}

data "iceberg_tables" "read" {
  namespace = iceberg_namespace.db.name

  depends_on = [
    iceberg_table.alpha,
    iceberg_table.beta,
  ]
}
`
}

func testAccIcebergTablesDataSourceNestedConfig(providerCfg string) string {
	return providerCfg + `
resource "iceberg_namespace" "db" {
  name = ["analytics", "raw"]
}

resource "iceberg_table" "events" {
  namespace = iceberg_namespace.db.name
  name      = "events"
  schema = {
    fields = [
      {
        id       = 1
        name     = "id"
        type     = "long"
        required = true
      }
    ]
  }
}

data "iceberg_tables" "read" {
  namespace = iceberg_namespace.db.name

  depends_on = [iceberg_table.events]
}
`
}

func testAccIcebergTablesDataSourceEmptyNamespaceConfig(providerCfg string) string {
	return providerCfg + `
resource "iceberg_namespace" "db" {
  name = ["ns_tables_ds_empty"]
}

data "iceberg_tables" "read" {
  namespace = iceberg_namespace.db.name
}
`
}

func testAccIcebergTablesDataSourceMissingConfig(providerCfg string) string {
	return providerCfg + `
data "iceberg_tables" "missing" {
  namespace = ["definitely_no_such_namespace"]
}
`
}
