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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

// testAccPolarisCatalogIntegrationProvider returns provider HCL and false if POLARIS_CATALOG_URI is unset.
func testAccPolarisCatalogIntegrationProvider() (cfg string, ok bool) {
	catalogURI := os.Getenv("POLARIS_CATALOG_URI")
	if catalogURI == "" {
		return "", false
	}
	managementURI := strings.TrimRight(catalogURI, "/") + "/api/management/v1"
	token := os.Getenv("POLARIS_TOKEN")

	return testAccPolarisProviderConfigWithToken(catalogURI, managementURI, token), true
}

// TestAccPolarisCatalog_Integration runs one subtest per storage type (FILE, S3, GCS, AZURE) against a real Polaris.
// Requires POLARIS_CATALOG_URI (and typically POLARIS_TOKEN from dev/provision_polaris.py). See make test-integration.
//
// Optional skips:
//   - POLARIS_SKIP_S3_CATALOG, POLARIS_SKIP_AZURE_CATALOG — set to "1" to skip that subtest.
//   - GCS is opt-in: set POLARIS_ENABLE_GCS_CATALOG_TEST=1 when Polaris has working GCP credentials (default
//     local docker image returns 500 "Failed to get GCP credentials" for GCS catalogs).
func TestAccPolarisCatalog_Integration(t *testing.T) {
	providerCfg, ok := testAccPolarisCatalogIntegrationProvider()
	if !ok {
		t.Skip("POLARIS_CATALOG_URI not set, skipping Polaris catalog integration tests")
	}

	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	t.Run("FILE", func(t *testing.T) {
		testAccPolarisCatalogIntegration_FILE(t, providerCfg, suffix)
	})
	t.Run("S3", func(t *testing.T) {
		if os.Getenv("POLARIS_SKIP_S3_CATALOG") == "1" {
			t.Skip("POLARIS_SKIP_S3_CATALOG=1")
		}
		testAccPolarisCatalogIntegration_S3(t, providerCfg, suffix)
	})
	t.Run("GCS", func(t *testing.T) {
		if os.Getenv("POLARIS_SKIP_GCS_CATALOG") == "1" {
			t.Skip("POLARIS_SKIP_GCS_CATALOG=1")
		}
		if os.Getenv("POLARIS_ENABLE_GCS_CATALOG_TEST") != "1" {
			t.Skip("set POLARIS_ENABLE_GCS_CATALOG_TEST=1 to run GCS (requires GCP credentials on the Polaris server)")
		}
		testAccPolarisCatalogIntegration_GCS(t, providerCfg, suffix)
	})
	t.Run("AZURE", func(t *testing.T) {
		if os.Getenv("POLARIS_SKIP_AZURE_CATALOG") == "1" {
			t.Skip("POLARIS_SKIP_AZURE_CATALOG=1")
		}
		testAccPolarisCatalogIntegration_AZURE(t, providerCfg, suffix)
	})
}

func testAccPolarisCatalogIntegration_FILE(t *testing.T, providerCfg, suffix string) {
	t.Helper()

	catName := "tf-file-" + suffix
	baseLoc := fmt.Sprintf("file:///tmp/polaris-tf-%s/warehouse", suffix)

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerCfg + fmt.Sprintf(`
resource "iceberg_polaris_catalog" "file" {
  name                  = %q
  default_base_location = %q

  storage_config = {
    storage_type      = "FILE"
    allowed_locations = [%[2]q]
  }

  properties = {
    integration = "true"
  }
}
`, catName, baseLoc),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.file", "id", catName),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.file", "name", catName),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.file", "default_base_location", baseLoc),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.file", "properties.integration", "true"),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.file", "properties.%", "1"),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.file", "storage_config.storage_type", "FILE"),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.file", "storage_config.allowed_locations.#", "1"),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.file", "storage_config.allowed_locations.0", baseLoc),
					resource.TestCheckResourceAttrSet("iceberg_polaris_catalog.file", "entity_version"),
				),
			},
			{
				Config: providerCfg + fmt.Sprintf(`
resource "iceberg_polaris_catalog" "file" {
  name                  = %q
  default_base_location = %q

  storage_config = {
    storage_type      = "FILE"
    allowed_locations = [%[2]q]
  }

  properties = {
    integration = "true"
    phase       = "two"
  }
}
`, catName, baseLoc),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.file", "properties.phase", "two"),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.file", "properties.%", "2"),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.file", "storage_config.storage_type", "FILE"),
				),
			},
		},
	})
}

func testAccPolarisCatalogIntegration_S3(t *testing.T, providerCfg, suffix string) {
	t.Helper()

	catName := "tf-s3-" + suffix
	baseLoc := fmt.Sprintf("s3://warehouse/tf-acc-%s/data/", suffix)
	roleARN := "arn:aws:iam::123456789012:role/polaris-tf-s3-" + suffix
	userARN := "arn:aws:iam::123456789012:user/polaris-tf-s3-" + suffix
	extID := "ext-id-" + suffix
	kmsKey := "arn:aws:kms:us-east-1:123456789012:key/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	kmsKey2 := "arn:aws:kms:us-east-1:123456789012:key/bbbbbbbb-cccc-dddd-eeee-ffffffffffff"
	// Polaris in docker-compose shares network with MinIO; server-side S3 calls use this host.
	endpoint := "http://minio:9000"
	internalEP := "http://minio:9000"

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerCfg + fmt.Sprintf(`
resource "iceberg_polaris_catalog" "s3" {
  name                  = %q
  default_base_location = %q

  storage_config = {
    storage_type       = "S3"
    allowed_locations  = [%[2]q]
    storage_name       = "tf-s3-store-%[3]s"
    role_arn           = %[4]q
    external_id        = %[5]q
    user_arn           = %[6]q
    current_kms_key    = %[7]q
    allowed_kms_keys   = [%[7]q, %[8]q]
    region             = "us-east-1"
    endpoint           = %[9]q
    sts_endpoint       = %[9]q
    endpoint_internal  = %[10]q
    sts_unavailable    = true
    path_style_access  = true
    kms_unavailable    = true
  }

  properties = {
    suite = "s3"
  }
}
`, catName, baseLoc, suffix, roleARN, extID, userARN, kmsKey, kmsKey2, endpoint, internalEP),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.s3", "id", catName),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.s3", "name", catName),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.s3", "default_base_location", baseLoc),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.s3", "properties.suite", "s3"),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.s3", "properties.%", "1"),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.s3", "storage_config.storage_type", "S3"),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.s3", "storage_config.allowed_locations.#", "1"),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.s3", "storage_config.allowed_locations.0", baseLoc),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.s3", "storage_config.storage_name", "tf-s3-store-"+suffix),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.s3", "storage_config.role_arn", roleARN),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.s3", "storage_config.external_id", extID),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.s3", "storage_config.user_arn", userARN),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.s3", "storage_config.current_kms_key", kmsKey),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.s3", "storage_config.allowed_kms_keys.#", "2"),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.s3", "storage_config.allowed_kms_keys.0", kmsKey),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.s3", "storage_config.allowed_kms_keys.1", kmsKey2),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.s3", "storage_config.region", "us-east-1"),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.s3", "storage_config.endpoint", endpoint),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.s3", "storage_config.sts_endpoint", endpoint),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.s3", "storage_config.endpoint_internal", internalEP),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.s3", "storage_config.sts_unavailable", "true"),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.s3", "storage_config.path_style_access", "true"),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.s3", "storage_config.kms_unavailable", "true"),
					resource.TestCheckResourceAttrSet("iceberg_polaris_catalog.s3", "entity_version"),
				),
			},
		},
	})
}

func testAccPolarisCatalogIntegration_GCS(t *testing.T, providerCfg, suffix string) {
	t.Helper()

	catName := "tf-gcs-" + suffix
	baseLoc := fmt.Sprintf("gs://tf-gcs-bucket-%s/prefix/", suffix)
	sa := fmt.Sprintf("polaris-tf-gcs-%s@project.iam.gserviceaccount.com", suffix)

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerCfg + fmt.Sprintf(`
resource "iceberg_polaris_catalog" "gcs" {
  name                  = %q
  default_base_location = %q

  storage_config = {
    storage_type        = "GCS"
    allowed_locations   = [%[2]q]
    storage_name        = "tf-gcs-store-%[3]s"
    gcs_service_account = %[4]q
  }

  properties = {
    suite = "gcs"
  }
}
`, catName, baseLoc, suffix, sa),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.gcs", "id", catName),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.gcs", "name", catName),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.gcs", "default_base_location", baseLoc),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.gcs", "properties.suite", "gcs"),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.gcs", "properties.%", "1"),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.gcs", "storage_config.storage_type", "GCS"),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.gcs", "storage_config.allowed_locations.#", "1"),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.gcs", "storage_config.allowed_locations.0", baseLoc),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.gcs", "storage_config.storage_name", "tf-gcs-store-"+suffix),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.gcs", "storage_config.gcs_service_account", sa),
					resource.TestCheckResourceAttrSet("iceberg_polaris_catalog.gcs", "entity_version"),
				),
			},
		},
	})
}

func testAccPolarisCatalogIntegration_AZURE(t *testing.T, providerCfg, suffix string) {
	t.Helper()

	catName := "tf-az-" + suffix
	baseLoc := fmt.Sprintf("abfss://container%s@storageaccount.dfs.core.windows.net/tf/", suffix)
	tenantID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	appName := "tf-azure-app-" + suffix
	consent := "https://login.microsoftonline.com/common/adminconsent"

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerCfg + fmt.Sprintf(`
resource "iceberg_polaris_catalog" "azure" {
  name                  = %q
  default_base_location = %q

  storage_config = {
    storage_type          = "AZURE"
    allowed_locations     = [%[2]q]
    storage_name          = "tf-azure-store-%[3]s"
    tenant_id             = %[4]q
    multi_tenant_app_name = %[5]q
    consent_url           = %[6]q
    hierarchical          = true
  }

  properties = {
    suite = "azure"
  }
}
`, catName, baseLoc, suffix, tenantID, appName, consent),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.azure", "id", catName),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.azure", "name", catName),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.azure", "default_base_location", baseLoc),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.azure", "properties.suite", "azure"),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.azure", "properties.%", "1"),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.azure", "storage_config.storage_type", "AZURE"),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.azure", "storage_config.allowed_locations.#", "1"),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.azure", "storage_config.allowed_locations.0", baseLoc),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.azure", "storage_config.storage_name", "tf-azure-store-"+suffix),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.azure", "storage_config.tenant_id", tenantID),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.azure", "storage_config.multi_tenant_app_name", appName),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.azure", "storage_config.consent_url", consent),
					resource.TestCheckResourceAttr("iceberg_polaris_catalog.azure", "storage_config.hierarchical", "true"),
					resource.TestCheckResourceAttrSet("iceberg_polaris_catalog.azure", "entity_version"),
				),
			},
		},
	})
}
