---
page_title: "iceberg_namespace Data Source - Iceberg"
subcategory: ""
description: |-
  Reads metadata for an existing Iceberg namespace from the catalog.
---

<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

# iceberg_namespace (Data Source)

Reads metadata for an existing Iceberg namespace from the catalog.

Use this data source to inspect namespaces created outside Terraform, to reference namespaces managed in another workspace, or to read the current catalog state of a namespace also managed by an [`iceberg_namespace` resource](../resources/namespace.md).

## Example Usage

### Read a namespace by name

```terraform
data "iceberg_namespace" "analytics" {
  name = ["analytics", "prod"]
}

output "analytics_namespace_id" {
  value = data.iceberg_namespace.analytics.id
}
```

### Read a namespace managed by this provider

```terraform
resource "iceberg_namespace" "example" {
  name = ["example_namespace"]
  user_properties = {
    description = "An example namespace"
  }
}

data "iceberg_namespace" "example" {
  name = iceberg_namespace.example.name
}
```

## Schema

### Required

- `name` (List of String) The name of the namespace.

### Read-Only

- `id` (String) Dot-separated full namespace identifier.
- `server_properties` (Map of String) Properties returned by the catalog for the namespace.

## Error handling

- If the namespace does not exist, apply fails with a **Namespace not found** error.
- If `catalog_uri` is not configured on the provider, apply fails with **Catalog not available**.
