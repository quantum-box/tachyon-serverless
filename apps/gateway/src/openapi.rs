//! OpenAPI document (utoipa derive over the handlers and api-types schemas).

use utoipa::OpenApi;
use utoipa::openapi::security::{ApiKey, ApiKeyValue, HttpAuthScheme, HttpBuilder, SecurityScheme};

use tachyon_serverless_api_types as api;

use crate::{handlers, trigger_handlers};

struct SecurityAddon;

impl utoipa::Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "bearer",
            SecurityScheme::Http(HttpBuilder::new().scheme(HttpAuthScheme::Bearer).build()),
        );
        // Webhook deliveries (PLT-4641) are authenticated by an HMAC signature
        // over the timestamp and the body, not by a bearer token.
        components.add_security_scheme(
            "webhook_signature",
            SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::with_description(
                "x-tachyon-webhook-signature",
                "v1=<hex HMAC-SHA256(trigger secret, \"{x-tachyon-webhook-timestamp}.{raw body}\")>",
            ))),
        );
    }
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Tachyon Serverless Gateway",
        description = "Management, invoke, logs and usage API of the Tachyon Serverless prototype. Colon-style action paths (`/v1/functions/{id}:invoke`, `/v1/invocations/{id}:cancel`) are accepted as aliases of the slash forms documented here.",
        license(name = "MIT")
    ),
    paths(
        handlers::healthz,
        handlers::readyz,
        handlers::metrics,
        handlers::openapi_json,
        handlers::provider_info,
        handlers::capacity_info,
        handlers::upload_artifact,
        handlers::create_function,
        handlers::list_functions,
        handlers::get_function,
        handlers::delete_function,
        handlers::create_revision,
        handlers::list_revisions,
        handlers::get_revision,
        handlers::update_alias,
        handlers::get_alias,
        handlers::list_aliases,
        handlers::invoke_json,
        handlers::invoke_async,
        handlers::http_path,
        handlers::list_invocations,
        handlers::get_invocation,
        handlers::cancel_invocation,
        handlers::invocation_logs,
        handlers::usage,
        handlers::usage_report,
        handlers::budget_report,
        handlers::internal_config,
        trigger_handlers::create_trigger,
        trigger_handlers::list_triggers,
        trigger_handlers::get_trigger,
        trigger_handlers::update_trigger,
        trigger_handlers::delete_trigger,
        trigger_handlers::list_trigger_fires,
        trigger_handlers::receive_webhook,
        crate::dead_letters::list_dead_letters,
        crate::dead_letters::get_dead_letter,
        crate::dead_letters::redrive,
    ),
    components(schemas(
        api::ErrorCode,
        api::ApiError,
        api::ApiErrorBody,
        api::ProviderInfo,
        api::ReuseInfo,
        api::CapacityInfo,
        api::NodeInfo,
        api::ResourceAmounts,
        api::EnvironmentCounts,
        api::QueueInfo,
        api::StartRateInfo,
        api::TenantCapacityInfo,
        api::RevisionCapacityInfo,
        api::ScaleEventInfo,
        api::ScalingInfo,
        api::EnvironmentReuseReport,
        api::ArtifactUploadResponse,
        api::CreateFunctionRequest,
        api::FunctionResponse,
        api::ArtifactRequest,
        api::ResourcesRequest,
        api::ExecutionRequest,
        api::SecretBindingRequest,
        api::EgressAllowRequest,
        api::CreateRevisionRequest,
        api::RevisionResponse,
        api::UpdateAliasRequest,
        api::AliasResponse,
        api::InvokeQuery,
        api::InvokeAsyncResponse,
        api::InvocationErrorResponse,
        api::TimingsResponse,
        api::AttemptResponse,
        api::InvocationResponse,
        api::AsyncDispatchResponse,
        api::DeadLetterResponse,
        api::RedriveRequestBody,
        api::RedriveResponse,
        api::RedriveAcceptedResponse,
        api::DeadlinesResponse,
        api::LogEntryResponse,
        api::LogsResponse,
        api::UsageSummaryResponse,
        api::UsageReportResponse,
        api::PriceTableInfo,
        api::UnitPricesMicros,
        api::UsageReportLine,
        api::UsageQuantities,
        api::OutcomeCounts,
        api::SegmentTotals,
        api::UnmeteredUsage,
        api::HostCostFacts,
        api::ProvisionalCharges,
        api::GuestReportedTotals,
        api::TriggerTargetRequest,
        api::MissedRunPolicyRequest,
        api::CronTriggerRequest,
        api::WebhookTriggerRequest,
        api::CreateTriggerRequest,
        api::UpdateTriggerRequest,
        api::CronTriggerInfo,
        api::WebhookTriggerInfo,
        api::TriggerResponse,
        api::TriggerFireResponse,
        api::WebhookAcceptedResponse,
        api::BudgetReportResponse,
        api::BudgetScopeReport,
        api::BudgetAlert,
    )),
    tags(
        (name = "meta"), (name = "provider"), (name = "artifacts"), (name = "functions"),
        (name = "revisions"), (name = "aliases"), (name = "invoke"), (name = "invocations"), (name = "usage"), (name = "triggers"), (name = "dead-letters"), (name = "budget"), (name = "internal")
    ),
    modifiers(&SecurityAddon)
)]
pub struct ApiDoc;

/// The document as JSON.
pub fn document() -> serde_json::Value {
    serde_json::to_value(ApiDoc::openapi()).unwrap_or_else(
        |e| serde_json::json!({ "error": format!("openapi serialization failed: {e}") }),
    )
}
