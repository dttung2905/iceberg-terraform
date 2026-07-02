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

var _ datasource.DataSource = &icebergNamespaceDataSource{}

func NewNamespaceDataSource() datasource.DataSource {
	return &icebergNamespaceDataSource{}
}

type icebergNamespaceDataSourceModel struct {
	ID               types.String `tfsdk:"id"`
	Name             types.List   `tfsdk:"name"`
	ServerProperties types.Map    `tfsdk:"server_properties"`
}

type icebergNamespaceDataSource struct {
	catalog  catalog.Catalog
	provider *icebergProvider
}

func (d *icebergNamespaceDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_namespace"
}

func (d *icebergNamespaceDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = dschema.Schema{
		Description: "Reads metadata for an existing Iceberg namespace from the catalog.",
		Attributes: map[string]dschema.Attribute{
			"id": dschema.StringAttribute{
				Description: "Dot-separated full namespace identifier.",
				Computed:    true,
			},
			"name": dschema.ListAttribute{
				Description: "The name of the namespace.",
				Required:    true,
				ElementType: types.StringType,
			},
			"server_properties": dschema.MapAttribute{
				Description: "Properties returned by the catalog for the namespace.",
				Computed:    true,
				ElementType: types.StringType,
			},
		},
	}
}

func (d *icebergNamespaceDataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
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

func (d *icebergNamespaceDataSource) configureCatalog(ctx context.Context, diags *diag.Diagnostics) {
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

func (d *icebergNamespaceDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	tflog.Info(ctx, "Reading iceberg_namespace data source")
	d.configureCatalog(ctx, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}

	var data icebergNamespaceDataSourceModel
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
	resp.Diagnostics.Append(data.Name.ElementsAs(ctx, &namespaceName, false)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if len(namespaceName) == 0 {
		resp.Diagnostics.AddError(
			"Invalid namespace name",
			"The name attribute must contain at least one namespace segment.",
		)

		return
	}

	namespaceIdent := catalog.ToIdentifier(namespaceName...)

	nsProps, err := d.catalog.LoadNamespaceProperties(ctx, namespaceIdent)
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchNamespace) {
			resp.Diagnostics.AddError(
				"Namespace not found",
				"No such namespace: "+strings.Join(namespaceIdent, "."),
			)

			return
		}
		resp.Diagnostics.AddError(
			"failed to load namespace",
			"namespace "+strings.Join(namespaceIdent, ".")+": "+err.Error(),
		)

		return
	}

	data.ID = types.StringValue(strings.Join(namespaceIdent, "."))

	serverProperties, diags := types.MapValueFrom(ctx, types.StringType, nsProps)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	data.ServerProperties = serverProperties

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}
