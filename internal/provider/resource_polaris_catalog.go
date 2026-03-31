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

	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

var (
	_ resource.Resource                = &polarisCatalogResource{}
	_ resource.ResourceWithImportState = &polarisCatalogResource{}
)

func NewPolarisCatalogResource() resource.Resource {
	return &polarisCatalogResource{}
}

type polarisCatalogResource struct {
	provider         *icebergProvider
	managementClient *polarisManagementClient
}

type polarisCatalogResourceModel struct {
	ID                  types.String                     `tfsdk:"id"`
	Name                types.String                     `tfsdk:"name"`
	DefaultBaseLocation types.String                     `tfsdk:"default_base_location"`
	Properties          types.Map                        `tfsdk:"properties"`
	StorageConfig       polarisCatalogStorageConfigModel `tfsdk:"storage_config"`
	EntityVersion       types.Int64                      `tfsdk:"entity_version"`
}

type polarisCatalogStorageConfigModel struct {
	StorageType      types.String `tfsdk:"storage_type"`
	AllowedLocations types.List   `tfsdk:"allowed_locations"`
	StorageName      types.String `tfsdk:"storage_name"`
	// S3
	RoleArn          types.String `tfsdk:"role_arn"`
	ExternalID       types.String `tfsdk:"external_id"`
	UserArn          types.String `tfsdk:"user_arn"`
	CurrentKmsKey    types.String `tfsdk:"current_kms_key"`
	AllowedKmsKeys   types.List   `tfsdk:"allowed_kms_keys"`
	Region           types.String `tfsdk:"region"`
	Endpoint         types.String `tfsdk:"endpoint"`
	StsEndpoint      types.String `tfsdk:"sts_endpoint"`
	StsUnavailable   types.Bool   `tfsdk:"sts_unavailable"`
	EndpointInternal types.String `tfsdk:"endpoint_internal"`
	PathStyleAccess  types.Bool   `tfsdk:"path_style_access"`
	KmsUnavailable   types.Bool   `tfsdk:"kms_unavailable"`
	// Azure
	TenantID           types.String `tfsdk:"tenant_id"`
	MultiTenantAppName types.String `tfsdk:"multi_tenant_app_name"`
	ConsentURL         types.String `tfsdk:"consent_url"`
	Hierarchical       types.Bool   `tfsdk:"hierarchical"`
	// GCS
	GcsServiceAccount types.String `tfsdk:"gcs_service_account"`
}

func (r *polarisCatalogResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_polaris_catalog"
}

func storageConfigNestedAttributes() map[string]schema.Attribute {
	return map[string]schema.Attribute{
		"storage_type": schema.StringAttribute{
			Description: "Polaris storage discriminator: S3, GCS, AZURE, or FILE (see Apache Polaris management API).",
			Required:    true,
			Validators: []validator.String{
				stringvalidator.OneOf("S3", "GCS", "AZURE", "FILE"),
			},
		},
		"allowed_locations": schema.ListAttribute{
			Description: "URI prefixes clients may use (e.g. s3://bucket/prefix/, gs://..., abfss://..., file://...).",
			Optional:    true,
			ElementType: types.StringType,
		},
		"storage_name": schema.StringAttribute{
			Description: "Optional name referencing a server-side storage configuration.",
			Optional:    true,
		},
		"role_arn": schema.StringAttribute{
			Description: "S3: IAM role ARN granting access to buckets.",
			Optional:    true,
		},
		"external_id": schema.StringAttribute{
			Description: "S3: optional external ID for the trust relationship.",
			Optional:    true,
		},
		"user_arn": schema.StringAttribute{
			Description: "S3: IAM user ARN used to assume the role.",
			Optional:    true,
		},
		"current_kms_key": schema.StringAttribute{
			Description: "S3: KMS key ARN for encryption.",
			Optional:    true,
		},
		"allowed_kms_keys": schema.ListAttribute{
			Description: "S3: KMS keys allowed for reading data.",
			Optional:    true,
			ElementType: types.StringType,
		},
		"region": schema.StringAttribute{
			Description: "S3: AWS region where data is stored.",
			Optional:    true,
		},
		"endpoint": schema.StringAttribute{
			Description: "S3: endpoint for S3 requests (e.g. MinIO).",
			Optional:    true,
		},
		"sts_endpoint": schema.StringAttribute{
			Description: "S3: STS endpoint for the Polaris server.",
			Optional:    true,
		},
		"sts_unavailable": schema.BoolAttribute{
			Description: "S3: if true, Polaris avoids STS when obtaining credentials.",
			Optional:    true,
		},
		"endpoint_internal": schema.StringAttribute{
			Description: "S3: endpoint used by the Polaris server (not exposed to REST clients).",
			Optional:    true,
		},
		"path_style_access": schema.BoolAttribute{
			Description: "S3: use path-style bucket addressing.",
			Optional:    true,
		},
		"kms_unavailable": schema.BoolAttribute{
			Description: "S3: if true, Polaris avoids adding KMS key policies.",
			Optional:    true,
		},
		"tenant_id": schema.StringAttribute{
			Description: "Azure: tenant ID (required for AZURE storage).",
			Optional:    true,
		},
		"multi_tenant_app_name": schema.StringAttribute{
			Description: "Azure: multi-tenant application name.",
			Optional:    true,
		},
		"consent_url": schema.StringAttribute{
			Description: "Azure: URL for permissions consent.",
			Optional:    true,
		},
		"hierarchical": schema.BoolAttribute{
			Description: "Azure: scope SAS tokens to hierarchical paths (ADLS Gen2).",
			Optional:    true,
		},
		"gcs_service_account": schema.StringAttribute{
			Description: "GCS: service account email used by Polaris.",
			Optional:    true,
		},
	}
}

func (r *polarisCatalogResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages a Polaris Iceberg catalog (management API). Supports INTERNAL catalogs with S3, GCS, Azure, or FILE storage per the Polaris OpenAPI spec.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed: true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"name": schema.StringAttribute{
				Description: "Catalog name.",
				Required:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"default_base_location": schema.StringAttribute{
				Description: "Maps to catalog property default-base-location (required by Polaris for INTERNAL catalogs).",
				Required:    true,
			},
			"properties": schema.MapAttribute{
				Description: "Additional catalog string properties (excluding default-base-location, which is set via default_base_location).",
				Optional:    true,
				ElementType: types.StringType,
			},
			"storage_config": schema.SingleNestedAttribute{
				Description: "StorageConfigInfo: discriminator storage_type plus provider-specific fields.",
				Required:    true,
				Attributes:  storageConfigNestedAttributes(),
			},
			"entity_version": schema.Int64Attribute{
				Description: "Polaris entity version (from the API).",
				Computed:    true,
			},
		},
	}
}

func (r *polarisCatalogResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	provider, ok := req.ProviderData.(*icebergProvider)
	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected Resource Configure Type",
			"Expected *icebergProvider, got a different type: %T. Please report this issue to the provider developers.",
		)

		return
	}
	r.provider = provider
}

func (r *polarisCatalogResource) ensureManagementClient(ctx context.Context, diags *diag.Diagnostics) {
	if r.managementClient != nil {
		return
	}
	if r.provider == nil {
		diags.AddError(
			"Provider not configured",
			"The provider hasn't been configured before this operation")

		return
	}
	client, err := r.provider.newPolarisManagementClient()
	if err != nil {
		diags.AddError("Failed to create Polaris management API client", err.Error())

		return
	}
	r.managementClient = client
}

func mergeCatalogProperties(defaultBase string, extra map[string]string) map[string]string {
	out := make(map[string]string)
	for k, v := range extra {
		out[k] = v
	}
	out["default-base-location"] = defaultBase

	return out
}

func splitCatalogProperties(props map[string]string) (defaultBase string, rest map[string]string) {
	rest = make(map[string]string)
	for k, v := range props {
		if k == "default-base-location" {
			defaultBase = v

			continue
		}
		rest[k] = v
	}

	return defaultBase, rest
}

func stringListFromTF(ctx context.Context, l types.List) ([]string, diag.Diagnostics) {
	var diags diag.Diagnostics
	if l.IsNull() || l.IsUnknown() {
		return nil, diags
	}
	var out []string
	diags = l.ElementsAs(ctx, &out, false)

	return out, diags
}

func optionalStringPtr(s types.String) *string {
	if s.IsNull() || s.IsUnknown() {
		return nil
	}
	v := s.ValueString()
	if v == "" {
		return nil
	}

	return &v
}

func optionalBoolPtr(b types.Bool) *bool {
	if b.IsNull() || b.IsUnknown() {
		return nil
	}
	v := b.ValueBool()

	return &v
}

func typesStringFromOptionalPtr(p *string) types.String {
	if p == nil || *p == "" {
		return types.StringNull()
	}

	return types.StringValue(*p)
}

func typesBoolFromOptionalPtr(p *bool) types.Bool {
	if p == nil {
		return types.BoolNull()
	}

	return types.BoolValue(*p)
}

func validatePolarisCatalogStorageConfig(m polarisCatalogStorageConfigModel) diag.Diagnostics {
	var diags diag.Diagnostics
	switch m.StorageType.ValueString() {
	case "AZURE":
		if m.TenantID.IsNull() || m.TenantID.IsUnknown() || m.TenantID.ValueString() == "" {
			diags.AddError(
				"Invalid storage_config",
				"tenant_id is required when storage_type is AZURE.",
			)
		}
	default:
	}

	return diags
}

func storageConfigFromModel(ctx context.Context, m polarisCatalogStorageConfigModel) (polarisStorageConfigInfo, diag.Diagnostics) {
	var diags diag.Diagnostics
	st := m.StorageType.ValueString()
	cfg := polarisStorageConfigInfo{StorageType: st}

	loc, d := stringListFromTF(ctx, m.AllowedLocations)
	diags.Append(d...)
	cfg.AllowedLocations = loc

	if !m.StorageName.IsNull() && !m.StorageName.IsUnknown() {
		cfg.StorageName = m.StorageName.ValueString()
	}

	cfg.RoleArn = optionalStringPtr(m.RoleArn)
	cfg.ExternalID = optionalStringPtr(m.ExternalID)
	cfg.UserArn = optionalStringPtr(m.UserArn)
	cfg.CurrentKmsKey = optionalStringPtr(m.CurrentKmsKey)
	keys, d := stringListFromTF(ctx, m.AllowedKmsKeys)
	diags.Append(d...)
	cfg.AllowedKmsKeys = keys

	cfg.Region = optionalStringPtr(m.Region)
	cfg.Endpoint = optionalStringPtr(m.Endpoint)
	cfg.StsEndpoint = optionalStringPtr(m.StsEndpoint)
	cfg.StsUnavailable = optionalBoolPtr(m.StsUnavailable)
	cfg.EndpointInternal = optionalStringPtr(m.EndpointInternal)
	cfg.PathStyleAccess = optionalBoolPtr(m.PathStyleAccess)
	cfg.KmsUnavailable = optionalBoolPtr(m.KmsUnavailable)

	cfg.TenantID = optionalStringPtr(m.TenantID)
	cfg.MultiTenantAppName = optionalStringPtr(m.MultiTenantAppName)
	cfg.ConsentURL = optionalStringPtr(m.ConsentURL)
	cfg.Hierarchical = optionalBoolPtr(m.Hierarchical)
	cfg.GcsServiceAccount = optionalStringPtr(m.GcsServiceAccount)

	return cfg, diags
}

func storageConfigToModel(ctx context.Context, cfg polarisStorageConfigInfo) (polarisCatalogStorageConfigModel, diag.Diagnostics) {
	var diags diag.Diagnostics
	m := polarisCatalogStorageConfigModel{
		StorageType: types.StringValue(cfg.StorageType),
	}

	if len(cfg.AllowedLocations) > 0 {
		l, d := types.ListValueFrom(ctx, types.StringType, cfg.AllowedLocations)
		diags.Append(d...)
		m.AllowedLocations = l
	} else {
		m.AllowedLocations = types.ListNull(types.StringType)
	}

	if cfg.StorageName != "" {
		m.StorageName = types.StringValue(cfg.StorageName)
	} else {
		m.StorageName = types.StringNull()
	}

	m.RoleArn = typesStringFromOptionalPtr(cfg.RoleArn)
	m.ExternalID = typesStringFromOptionalPtr(cfg.ExternalID)
	m.UserArn = typesStringFromOptionalPtr(cfg.UserArn)
	m.CurrentKmsKey = typesStringFromOptionalPtr(cfg.CurrentKmsKey)

	if len(cfg.AllowedKmsKeys) > 0 {
		l, d := types.ListValueFrom(ctx, types.StringType, cfg.AllowedKmsKeys)
		diags.Append(d...)
		m.AllowedKmsKeys = l
	} else {
		m.AllowedKmsKeys = types.ListNull(types.StringType)
	}

	m.Region = typesStringFromOptionalPtr(cfg.Region)
	m.Endpoint = typesStringFromOptionalPtr(cfg.Endpoint)
	m.StsEndpoint = typesStringFromOptionalPtr(cfg.StsEndpoint)
	m.StsUnavailable = typesBoolFromOptionalPtr(cfg.StsUnavailable)
	m.EndpointInternal = typesStringFromOptionalPtr(cfg.EndpointInternal)
	m.PathStyleAccess = typesBoolFromOptionalPtr(cfg.PathStyleAccess)
	m.KmsUnavailable = typesBoolFromOptionalPtr(cfg.KmsUnavailable)

	m.TenantID = typesStringFromOptionalPtr(cfg.TenantID)
	m.MultiTenantAppName = typesStringFromOptionalPtr(cfg.MultiTenantAppName)
	m.ConsentURL = typesStringFromOptionalPtr(cfg.ConsentURL)
	m.Hierarchical = typesBoolFromOptionalPtr(cfg.Hierarchical)
	m.GcsServiceAccount = typesStringFromOptionalPtr(cfg.GcsServiceAccount)

	return m, diags
}

func syncPolarisCatalogStateFromAPI(ctx context.Context, data *polarisCatalogResourceModel, cat *polarisCatalog) diag.Diagnostics {
	var diags diag.Diagnostics

	data.ID = types.StringValue(cat.Name)
	data.Name = types.StringValue(cat.Name)
	data.EntityVersion = types.Int64Value(cat.EntityVersion)

	base, rest := splitCatalogProperties(cat.Properties)
	data.DefaultBaseLocation = types.StringValue(base)

	if len(rest) > 0 {
		propsVal, d := types.MapValueFrom(ctx, types.StringType, rest)
		diags.Append(d...)
		data.Properties = propsVal
	} else {
		data.Properties = types.MapNull(types.StringType)
	}

	sc, d := storageConfigToModel(ctx, cat.StorageConfigInfo)
	diags.Append(d...)
	data.StorageConfig = sc

	return diags
}

// mergeStorageConfigFillNullsFromSource copies each storage_config field from src into dst when dst is null/unknown.
// Polaris GET responses often omit optional storageConfigInfo fields that were sent on create/update; without this,
// Terraform reports "Provider produced inconsistent result after apply" when refreshed state does not match the plan.
func mergeStorageConfigFillNullsFromSource(dst *polarisCatalogStorageConfigModel, src polarisCatalogStorageConfigModel) {
	mergeString := func(d *types.String, s types.String) {
		if d.IsNull() || d.IsUnknown() {
			if !s.IsNull() && !s.IsUnknown() {
				*d = s
			}
		}
	}
	mergeBool := func(d *types.Bool, s types.Bool) {
		if d.IsNull() || d.IsUnknown() {
			if !s.IsNull() && !s.IsUnknown() {
				*d = s
			}
		}
	}
	mergeList := func(d *types.List, s types.List) {
		if d.IsNull() || d.IsUnknown() {
			if !s.IsNull() && !s.IsUnknown() {
				*d = s
			}
		}
	}

	mergeString(&dst.StorageType, src.StorageType)
	mergeList(&dst.AllowedLocations, src.AllowedLocations)
	mergeString(&dst.StorageName, src.StorageName)
	mergeString(&dst.RoleArn, src.RoleArn)
	mergeString(&dst.ExternalID, src.ExternalID)
	mergeString(&dst.UserArn, src.UserArn)
	mergeString(&dst.CurrentKmsKey, src.CurrentKmsKey)
	mergeList(&dst.AllowedKmsKeys, src.AllowedKmsKeys)
	mergeString(&dst.Region, src.Region)
	mergeString(&dst.Endpoint, src.Endpoint)
	mergeString(&dst.StsEndpoint, src.StsEndpoint)
	mergeBool(&dst.StsUnavailable, src.StsUnavailable)
	mergeString(&dst.EndpointInternal, src.EndpointInternal)
	mergeBool(&dst.PathStyleAccess, src.PathStyleAccess)
	mergeBool(&dst.KmsUnavailable, src.KmsUnavailable)
	mergeString(&dst.TenantID, src.TenantID)
	mergeString(&dst.MultiTenantAppName, src.MultiTenantAppName)
	mergeString(&dst.ConsentURL, src.ConsentURL)
	mergeBool(&dst.Hierarchical, src.Hierarchical)
	mergeString(&dst.GcsServiceAccount, src.GcsServiceAccount)
}

func (r *polarisCatalogResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	r.ensureManagementClient(ctx, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}

	var data polarisCatalogResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(validatePolarisCatalogStorageConfig(data.StorageConfig)...)
	if resp.Diagnostics.HasError() {
		return
	}

	extra := make(map[string]string)
	if !data.Properties.IsNull() && !data.Properties.IsUnknown() {
		resp.Diagnostics.Append(data.Properties.ElementsAs(ctx, &extra, false)...)
		if resp.Diagnostics.HasError() {
			return
		}
	}

	sc, d := storageConfigFromModel(ctx, data.StorageConfig)
	resp.Diagnostics.Append(d...)
	if resp.Diagnostics.HasError() {
		return
	}

	name := data.Name.ValueString()
	props := mergeCatalogProperties(data.DefaultBaseLocation.ValueString(), extra)

	createReq := polarisCreateCatalogRequest{
		Catalog: polarisCatalog{
			Type:              "INTERNAL",
			Name:              name,
			Properties:        props,
			StorageConfigInfo: sc,
		},
	}

	tflog.Info(ctx, "Creating Polaris catalog", map[string]any{"name": name})

	created, err := r.managementClient.CreateCatalog(ctx, createReq)
	if err != nil {
		resp.Diagnostics.AddError("failed to create catalog", err.Error())

		return
	}

	planStorage := data.StorageConfig
	resp.Diagnostics.Append(syncPolarisCatalogStateFromAPI(ctx, &data, created)...)
	mergeStorageConfigFillNullsFromSource(&data.StorageConfig, planStorage)
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *polarisCatalogResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	r.ensureManagementClient(ctx, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}

	var data polarisCatalogResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	name := data.Name.ValueString()
	tflog.Info(ctx, "Reading Polaris catalog", map[string]any{"name": name})

	cat, err := r.managementClient.GetCatalog(ctx, name)
	if err != nil {
		if isPolarisNotFoundError(err) {
			resp.State.RemoveResource(ctx)

			return
		}
		resp.Diagnostics.AddError("Failed to read Polaris catalog", err.Error())

		return
	}

	if cat.Type != "INTERNAL" {
		resp.Diagnostics.AddWarning(
			"Unsupported catalog type in Terraform state",
			"This resource only manages INTERNAL catalogs; the API returned type "+cat.Type+". State may not round-trip.",
		)
	}

	priorStorage := data.StorageConfig
	resp.Diagnostics.Append(syncPolarisCatalogStateFromAPI(ctx, &data, cat)...)
	mergeStorageConfigFillNullsFromSource(&data.StorageConfig, priorStorage)
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *polarisCatalogResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	r.ensureManagementClient(ctx, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}

	var plan, state polarisCatalogResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(validatePolarisCatalogStorageConfig(plan.StorageConfig)...)
	if resp.Diagnostics.HasError() {
		return
	}

	name := state.Name.ValueString()

	current, err := r.managementClient.GetCatalog(ctx, name)
	if err != nil {
		var nf *polarisNotFoundError
		if errors.As(err, &nf) {
			resp.State.RemoveResource(ctx)

			return
		}
		resp.Diagnostics.AddError("Failed to read Polaris catalog for update", err.Error())

		return
	}

	extra := make(map[string]string)
	if !plan.Properties.IsNull() && !plan.Properties.IsUnknown() {
		resp.Diagnostics.Append(plan.Properties.ElementsAs(ctx, &extra, false)...)
		if resp.Diagnostics.HasError() {
			return
		}
	}

	sc, d := storageConfigFromModel(ctx, plan.StorageConfig)
	resp.Diagnostics.Append(d...)
	if resp.Diagnostics.HasError() {
		return
	}

	props := mergeCatalogProperties(plan.DefaultBaseLocation.ValueString(), extra)
	updateReq := polarisUpdateCatalogRequest{
		CurrentEntityVersion: current.EntityVersion,
		Properties:           props,
		StorageConfigInfo:    &sc,
	}

	tflog.Info(ctx, "Updating Polaris catalog", map[string]any{"name": name})

	updated, err := r.managementClient.UpdateCatalog(ctx, name, updateReq)
	if err != nil {
		var nf *polarisNotFoundError
		if errors.As(err, &nf) {
			resp.State.RemoveResource(ctx)

			return
		}
		resp.Diagnostics.AddError("Failed to update Polaris catalog", err.Error())

		return
	}

	planStorage := plan.StorageConfig
	resp.Diagnostics.Append(syncPolarisCatalogStateFromAPI(ctx, &state, updated)...)
	mergeStorageConfigFillNullsFromSource(&state.StorageConfig, planStorage)
	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}

func (r *polarisCatalogResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	r.ensureManagementClient(ctx, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}

	var data polarisCatalogResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	name := data.Name.ValueString()
	tflog.Info(ctx, "Deleting Polaris catalog", map[string]any{"name": name})

	err := r.managementClient.DeleteCatalog(ctx, name)
	if err != nil && !isPolarisNotFoundError(err) {
		resp.Diagnostics.AddError("Failed to delete Polaris catalog", err.Error())

		return
	}
}

func (r *polarisCatalogResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), req.ID)...)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("name"), req.ID)...)
}
