// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

resource "iceberg_namespace" "example" {
  name = ["example_namespace"]
}

resource "iceberg_table" "example" {
  namespace = iceberg_namespace.example.name
  name      = "example_table"

  schema = {
    fields = [
      {
        name     = "id"
        type     = "long"
        required = true
      },
      {
        name     = "data"
        type     = "string"
        required = false
      }
    ]
  }
}

data "iceberg_table" "example" {
  depends_on = [iceberg_table.example]

  namespace = iceberg_namespace.example.name
  name      = iceberg_table.example.name
}

output "table_id" {
  value = data.iceberg_table.example.id
}

output "table_schema_field_names" {
  value = [for f in data.iceberg_table.example.schema.fields : f.name]
}

output "table_server_properties" {
  value = data.iceberg_table.example.server_properties
}
