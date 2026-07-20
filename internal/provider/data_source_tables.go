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
	"slices"
	"strings"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	dschema "github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

var _ datasource.DataSource = &icebergTablesDataSource{}

func NewTablesDataSource() datasource.DataSource {
	return &icebergTablesDataSource{}
}

type icebergTablesDataSourceModel struct {
	ID          types.String `tfsdk:"id"`
	Namespace   types.List   `tfsdk:"namespace"`
	Tables      types.List   `tfsdk:"tables"`
	Identifiers types.List   `tfsdk:"identifiers"`
}

type icebergTablesDataSource struct {
	catalog  catalog.Catalog
	provider *icebergProvider
}

func (d *icebergTablesDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_tables"
}

func (d *icebergTablesDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = dschema.Schema{
		Description: "Lists table names in an Iceberg namespace from the catalog.",
		Attributes: map[string]dschema.Attribute{
			"id": dschema.StringAttribute{
				Description: "Dot-separated full namespace identifier.",
				Computed:    true,
			},
			"namespace": dschema.ListAttribute{
				Description: "The namespace to list tables in.",
				Required:    true,
				ElementType: types.StringType,
			},
			"tables": dschema.ListAttribute{
				Description: "Table names in the namespace, without namespace segments.",
				Computed:    true,
				ElementType: types.StringType,
			},
			"identifiers": dschema.ListAttribute{
				Description: "Dot-separated full table identifiers (namespace segments + table name), matching iceberg_table id format.",
				Computed:    true,
				ElementType: types.StringType,
			},
		},
	}
}

func (d *icebergTablesDataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
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

func (d *icebergTablesDataSource) configureCatalog(ctx context.Context, diags *diag.Diagnostics) {
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

// identifierString formats a table identifier as a dot-separated string.
// table.Identifier is currently a []string alias with no String() method;
// keep this helper so formatting stays in one place if that changes before v1.0.
func identifierString(ident table.Identifier) string {
	return strings.Join(ident, ".")
}

func sortTableIdentifiers(identifiers []table.Identifier) {
	// Stable sort keeps list order deterministic across refreshes so Terraform
	// does not report spurious diffs when the catalog returns tables unordered.
	slices.SortStableFunc(identifiers, func(a, b table.Identifier) int {
		return strings.Compare(identifierString(a), identifierString(b))
	})
}

func tableNamesFromIdentifiers(identifiers []table.Identifier) []string {
	names := make([]string, 0, len(identifiers))
	for _, ident := range identifiers {
		names = append(names, catalog.TableNameFromIdent(ident))
	}

	return names
}

func tableIdentifierStrings(identifiers []table.Identifier) []string {
	out := make([]string, 0, len(identifiers))
	for _, ident := range identifiers {
		out = append(out, identifierString(ident))
	}

	return out
}

func (d *icebergTablesDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	tflog.Info(ctx, "Reading iceberg_tables data source")
	d.configureCatalog(ctx, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}

	var data icebergTablesDataSourceModel
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

	var namespaceName []string
	resp.Diagnostics.Append(data.Namespace.ElementsAs(ctx, &namespaceName, false)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if len(namespaceName) == 0 {
		resp.Diagnostics.AddError(
			"Invalid namespace",
			"The namespace attribute must contain at least one namespace segment.",
		)

		return
	}

	namespaceIdent := catalog.ToIdentifier(namespaceName...)

	var tableIdents []table.Identifier
	for ident, err := range d.catalog.ListTables(ctx, namespaceIdent) {
		if err != nil {
			if errors.Is(err, catalog.ErrNoSuchNamespace) {
				resp.Diagnostics.AddError(
					"Namespace not found",
					"No such namespace: "+identifierString(namespaceIdent),
				)

				return
			}
			resp.Diagnostics.AddError("failed to list tables", err.Error())

			return
		}
		tableIdents = append(tableIdents, ident)
	}

	sortTableIdentifiers(tableIdents)
	tableNames := tableNamesFromIdentifiers(tableIdents)
	identifierStrings := tableIdentifierStrings(tableIdents)

	tables, diags := types.ListValueFrom(ctx, types.StringType, tableNames)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	identifiers, diags := types.ListValueFrom(ctx, types.StringType, identifierStrings)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	data.ID = types.StringValue(identifierString(namespaceIdent))
	data.Tables = tables
	data.Identifiers = identifiers

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}
