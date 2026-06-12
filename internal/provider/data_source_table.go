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
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/iceberg-go/catalog"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	dschema "github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

var _ datasource.DataSource = &icebergTableDataSource{}

func NewTableDataSource() datasource.DataSource {
	return &icebergTableDataSource{}
}

type icebergTableDataSourceModel struct {
	ID               types.String `tfsdk:"id"`
	Namespace        types.List   `tfsdk:"namespace"`
	Name             types.String `tfsdk:"name"`
	Schema           types.Object `tfsdk:"schema"`
	PartitionSpec    types.Object `tfsdk:"partition_spec"`
	SortOrder        types.Object `tfsdk:"sort_order"`
	ServerProperties types.Map    `tfsdk:"server_properties"`
}

type icebergTableDataSource struct {
	catalog  catalog.Catalog
	provider *icebergProvider
}

func (d *icebergTableDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_table"
}

func (d *icebergTableDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = dschema.Schema{
		Description: "Reads metadata for an existing Iceberg table from the catalog.",
		Attributes: map[string]dschema.Attribute{
			"id": dschema.StringAttribute{
				Description: "Dot-separated full table identifier (namespace segments + name).",
				Computed:    true,
			},
			"namespace": dschema.ListAttribute{
				Description: "The namespace of the table.",
				Required:    true,
				ElementType: types.StringType,
			},
			"name": dschema.StringAttribute{
				Description: "The name of the table.",
				Required:    true,
			},
			"schema": dschema.SingleNestedAttribute{
				Description: "The current schema of the table.",
				Computed:    true,
				Attributes:  tableSchemaDataSourceAttributes(),
			},
			"partition_spec": dschema.SingleNestedAttribute{
				Description: "The partition spec of the table; fields is empty when unpartitioned.",
				Computed:    true,
				Attributes:  tablePartitionSpecDataSourceAttributes(),
			},
			"sort_order": dschema.SingleNestedAttribute{
				Description: "The sort order of the table; fields is empty when unsorted.",
				Computed:    true,
				Attributes:  tableSortOrderDataSourceAttributes(),
			},
			"server_properties": dschema.MapAttribute{
				Description: "Properties from table metadata as returned by the catalog.",
				Computed:    true,
				ElementType: types.StringType,
			},
		},
	}
}

func (d *icebergTableDataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	provider, ok := req.ProviderData.(*icebergProvider)
	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected Data Source Configure Type",
			fmt.Sprintf("Expected *icebergProvider, got: %T. Please report this issue to the provider developers.", req.ProviderData),
		)

		return
	}

	d.provider = provider
}

func (d *icebergTableDataSource) configureCatalog(ctx context.Context, diags *diag.Diagnostics) {
	if d.catalog != nil {
		return
	}

	if d.provider == nil {
		diags.AddError(
			"Provider not configured",
			"The provider hasn't been configured before this operation",
		)

		return
	}

	if d.provider.catalogURI == "" {
		return
	}

	cat, err := d.provider.NewCatalog(ctx)
	if err != nil {
		diags.AddError(
			"Failed to create catalog",
			"Failed to create catalog: "+err.Error(),
		)

		return
	}
	d.catalog = cat
}

func (d *icebergTableDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	tflog.Info(ctx, "Reading iceberg_table data source")
	d.configureCatalog(ctx, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}

	var data icebergTableDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if d.catalog == nil {
		resp.Diagnostics.AddError(
			"Catalog not available",
			"The catalog could not be created (is catalog_uri set?).",
		)

		return
	}

	if data.Name.IsNull() || data.Name.IsUnknown() {
		resp.Diagnostics.AddError(
			"Invalid table name",
			"The name attribute must be set to a non-null, known value.",
		)

		return
	}

	var namespaceName []string
	resp.Diagnostics.Append(data.Namespace.ElementsAs(ctx, &namespaceName, false)...)
	if resp.Diagnostics.HasError() {
		return
	}

	tableName := data.Name.ValueString()
	tableIdent := append(namespaceName, tableName)

	tbl, err := d.catalog.LoadTable(ctx, tableIdent)
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchTable) {
			resp.Diagnostics.AddError(
				"Table not found",
				"No such table: "+strings.Join(tableIdent, "."),
			)

			return
		}
		resp.Diagnostics.AddError("failed to load table", err.Error())

		return
	}

	data.ID = types.StringValue(strings.Join(tableIdent, "."))

	meta := icebergTableMetadataFields{}
	convertIcebergTableMetadataToModel(ctx, tbl, &meta, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}
	data.Schema = meta.Schema
	data.PartitionSpec = meta.PartitionSpec
	data.SortOrder = meta.SortOrder
	data.ServerProperties = meta.ServerProperties

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}
