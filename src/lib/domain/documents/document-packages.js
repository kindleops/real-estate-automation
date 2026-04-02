import crypto from "node:crypto";

import { recordSystemAlert } from "@/lib/domain/alerts/system-alerts.js";
import { child } from "@/lib/logging/logger.js";
import { getStorageConfigSummary, getSignedUrl, uploadFile } from "@/lib/providers/storage.js";

const logger = child({
  module: "domain.documents.document_packages",
});

const DEFAULT_SIGNED_TTL_SECONDS = 7 * 24 * 60 * 60;

function clean(value) {
  return String(value ?? "").trim();
}

function slug(value, fallback = "document") {
  return (
    clean(value)
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "") ||
    fallback
  );
}

function nowIso() {
  return new Date().toISOString();
}

function safeArray(value) {
  return Array.isArray(value) ? value.filter(Boolean) : [];
}

function buildPackageRootKey({
  namespace = "packages",
  entity_type = "entity",
  entity_id = "unknown",
  label = "package",
} = {}) {
  const stamp = nowIso().replace(/[:.]/g, "-");
  const random = crypto.randomUUID().slice(0, 8);

  return [
    slug(namespace, "packages"),
    `${slug(entity_type, "entity")}-${slug(entity_id, "unknown")}`,
    `${stamp}-${slug(label, "package")}-${random}`,
  ].join("/");
}

function normalizeFile(file = {}, index = 0) {
  const filename =
    clean(file.filename || file.name) ||
    `document-${index + 1}.txt`;
  const content_type =
    clean(file.content_type || file.mime_type) ||
    (filename.endsWith(".json") ? "application/json" : "text/plain; charset=utf-8");
  const body =
    file.body ??
    file.contents ??
    file.text ??
    "";
  const body_encoding =
    clean(file.body_encoding || file.encoding) || "utf8";

  return {
    filename,
    key_name: clean(file.key_name) || filename,
    content_type,
    body,
    body_encoding,
    metadata: file.metadata && typeof file.metadata === "object" ? file.metadata : {},
  };
}

async function maybeAttachSignedUrl(file = {}, {
  expires_in_seconds = DEFAULT_SIGNED_TTL_SECONDS,
  dry_run = false,
} = {}) {
  if (!file?.key || dry_run) return file;

  const signed = await getSignedUrl({
    key: file.key,
    expires_in_seconds,
    disposition: "inline",
    filename: file.filename,
  });

  return {
    ...file,
    access_url: signed?.ok ? signed.url : null,
    access_path: signed?.ok ? signed.path : null,
    access_reason: signed?.reason || null,
  };
}

export async function createStoredDocumentPackage({
  namespace = "packages",
  entity_type = "entity",
  entity_id = "unknown",
  label = "package",
  metadata = {},
  files = [],
  signed_ttl_seconds = DEFAULT_SIGNED_TTL_SECONDS,
  dry_run = false,
} = {}) {
  const normalized_files = safeArray(files).map(normalizeFile).filter((file) => file.filename);

  if (!normalized_files.length) {
    await recordSystemAlert({
      subsystem: "storage",
      code: "document_package_missing_files",
      severity: "warning",
      retryable: false,
      summary: "Document package creation was attempted without any files.",
      dedupe_key: `document-package:${clean(entity_type)}:${clean(entity_id)}:missing-files`,
      metadata: {
        namespace,
        entity_type,
        entity_id,
        label,
      },
    });

    return {
      ok: false,
      dry_run,
      reason: "missing_document_files",
      package_id: null,
    };
  }

  const package_root_key = buildPackageRootKey({
    namespace,
    entity_type,
    entity_id,
    label,
  });
  const package_id = package_root_key.split("/").slice(-1)[0];
  const storage = getStorageConfigSummary();

  logger.info("document_package.create_requested", {
    namespace,
    entity_type,
    entity_id,
    label,
    files_count: normalized_files.length,
    dry_run,
  });

  if (dry_run) {
    const preview_files = normalized_files.map((file) => ({
      key: `${package_root_key}/${slug(file.key_name, "document")}`,
      filename: file.filename,
      content_type: file.content_type,
      size_bytes: Buffer.byteLength(
        typeof file.body === "string" ? file.body : JSON.stringify(file.body ?? ""),
        file.body_encoding === "base64" ? "base64" : "utf8"
      ),
    }));

    return {
      ok: true,
      dry_run: true,
      package_id,
      package_root_key,
      manifest_key: `${package_root_key}/manifest.json`,
      manifest_access_url: null,
      manifest_access_path: null,
      files: preview_files,
      primary_file: preview_files[0] || null,
      storage,
      metadata,
    };
  }

  const uploaded_files = [];

  for (const file of normalized_files) {
    const key = `${package_root_key}/${slug(file.key_name, "document")}`;
    const uploaded = await uploadFile({
      key,
      body: file.body,
      body_encoding: file.body_encoding,
      content_type: file.content_type,
      filename: file.filename,
      metadata: {
        package_id,
        package_root_key,
        label,
        entity_type,
        entity_id,
        ...(file.metadata || {}),
      },
      dry_run: false,
    });

    if (!uploaded?.ok) {
      await recordSystemAlert({
        subsystem: "storage",
        code: "document_upload_failed",
        severity: "high",
        retryable: true,
        summary: `Document upload failed for ${file.filename}.`,
        dedupe_key: `document-upload:${clean(entity_type)}:${clean(entity_id)}`,
        metadata: {
          namespace,
          entity_type,
          entity_id,
          label,
          failed_file: file.filename,
          upload_reason: uploaded?.reason || "document_file_upload_failed",
        },
      });

      return {
        ok: false,
        dry_run: false,
        reason: uploaded?.reason || "document_file_upload_failed",
        package_id,
        package_root_key,
        failed_file: file.filename,
      };
    }

    uploaded_files.push({
      key: uploaded.key,
      filename: uploaded.filename,
      content_type: uploaded.content_type,
      size_bytes: uploaded.size_bytes,
      sha256: uploaded.sha256,
      metadata: uploaded.metadata,
    });
  }

  const manifest_key = `${package_root_key}/manifest.json`;
  const manifest = {
    package_id,
    package_root_key,
    namespace: clean(namespace),
    entity_type: clean(entity_type),
    entity_id: clean(entity_id),
    label: clean(label),
    created_at: nowIso(),
    storage,
    metadata: metadata && typeof metadata === "object" ? metadata : {},
    files: uploaded_files,
  };

  const manifest_upload = await uploadFile({
    key: manifest_key,
    body: JSON.stringify(manifest, null, 2),
    content_type: "application/json",
    filename: "manifest.json",
    metadata: {
      package_id,
      package_root_key,
      label,
      entity_type,
      entity_id,
      manifest: true,
    },
  });

  if (!manifest_upload?.ok) {
    await recordSystemAlert({
      subsystem: "storage",
      code: "document_manifest_upload_failed",
      severity: "high",
      retryable: true,
      summary: "Document package manifest upload failed.",
      dedupe_key: `document-manifest:${clean(entity_type)}:${clean(entity_id)}`,
      metadata: {
        namespace,
        entity_type,
        entity_id,
        label,
        package_id,
        package_root_key,
        upload_reason: manifest_upload?.reason || "document_manifest_upload_failed",
      },
    });

    return {
      ok: false,
      dry_run: false,
      reason: manifest_upload?.reason || "document_manifest_upload_failed",
      package_id,
      package_root_key,
    };
  }

  const files_with_urls = [];
  for (const file of uploaded_files) {
    files_with_urls.push(await maybeAttachSignedUrl(file, {
      expires_in_seconds: signed_ttl_seconds,
      dry_run: false,
    }));
  }

  const manifest_signed = await getSignedUrl({
    key: manifest_key,
    expires_in_seconds: signed_ttl_seconds,
    disposition: "inline",
    filename: "manifest.json",
  });

  return {
    ok: true,
    dry_run: false,
    package_id,
    package_root_key,
    manifest_key,
    manifest_access_url: manifest_signed?.ok ? manifest_signed.url : null,
    manifest_access_path: manifest_signed?.ok ? manifest_signed.path : null,
    manifest_access_reason: manifest_signed?.reason || null,
    files: files_with_urls,
    primary_file: files_with_urls[0] || null,
    storage,
    metadata: manifest.metadata,
  };
}

export function buildBuyerDispositionPackageFiles({
  context = {},
  diagnostics = {},
} = {}) {
  const top_candidates = safeArray(diagnostics?.diagnostics?.top_candidates).slice(0, 8);
  const summary_lines = [
    `Property: ${clean(context.property_address) || "Unknown property"}`,
    clean(context.market_name) ? `Market: ${clean(context.market_name)}` : "",
    clean(context.zip_code) ? `ZIP: ${clean(context.zip_code)}` : "",
    clean(context.property_type) ? `Property Type: ${clean(context.property_type)}` : "",
    clean(context.disposition_strategy) ? `Disposition Strategy: ${clean(context.disposition_strategy)}` : "",
    context.units ? `Units: ${context.units}` : "",
    context.purchase_price ? `Acquisition Price: ${context.purchase_price}` : "",
    context.estimated_value ? `Estimated Value: ${context.estimated_value}` : "",
    clean(context.closing_date_target) ? `Target Close: ${clean(context.closing_date_target)}` : "",
    "",
    "Top Candidates:",
    ...(top_candidates.length
      ? top_candidates.map((candidate, index) => {
          const reasons = safeArray(candidate?.reasons).join(", ");
          return `${index + 1}. ${clean(candidate?.company_name) || `Buyer ${index + 1}`} (${Number(candidate?.score || 0)})${reasons ? ` - ${reasons}` : ""}`;
        })
      : ["No ranked buyers are available in the current intelligence snapshot."]),
  ].filter(Boolean);

  const package_json = {
    context,
    diagnostics: {
      viable_candidate_count: Number(diagnostics?.diagnostics?.viable_candidate_count || 0),
      total_candidates_evaluated: Number(diagnostics?.diagnostics?.total_candidates_evaluated || 0),
      top_candidates,
    },
  };

  return [
    {
      filename: "buyer-package-summary.txt",
      key_name: "buyer-package-summary.txt",
      content_type: "text/plain; charset=utf-8",
      body: summary_lines.join("\n"),
    },
    {
      filename: "buyer-package.json",
      key_name: "buyer-package.json",
      content_type: "application/json",
      body: JSON.stringify(package_json, null, 2),
    },
  ];
}

export function buildTitleIntroPackageFiles({
  title_company = {},
  contract_context = {},
  title_routing_item_id = null,
  closing_item_id = null,
  email = {},
} = {}) {
  const summary_lines = [
    `Title Company: ${clean(title_company.company_name) || "Unknown company"}`,
    clean(title_company.contact_name) ? `Contact: ${clean(title_company.contact_name)}` : "",
    clean(title_company.email) ? `Email: ${clean(title_company.email)}` : "",
    clean(contract_context.contract_id) ? `Contract ID: ${clean(contract_context.contract_id)}` : "",
    clean(contract_context.contract_title) ? `Reference: ${clean(contract_context.contract_title)}` : "",
    contract_context.purchase_price ? `Purchase Price: ${contract_context.purchase_price}` : "",
    clean(contract_context.closing_date_target) ? `Target Close: ${clean(contract_context.closing_date_target)}` : "",
    clean(contract_context.creative_terms) ? `Notes: ${clean(contract_context.creative_terms)}` : "",
    clean(title_routing_item_id) ? `Title Routing ID: ${clean(title_routing_item_id)}` : "",
    clean(closing_item_id) ? `Closing ID: ${clean(closing_item_id)}` : "",
    "",
    "Email Sent:",
    clean(email.subject) ? `Subject: ${clean(email.subject)}` : "",
    clean(email.body) || "",
  ].filter(Boolean);

  return [
    {
      filename: "title-intro-summary.txt",
      key_name: "title-intro-summary.txt",
      content_type: "text/plain; charset=utf-8",
      body: summary_lines.join("\n"),
    },
    {
      filename: "title-intro.json",
      key_name: "title-intro.json",
      content_type: "application/json",
      body: JSON.stringify(
        {
          title_company,
          contract_context,
          title_routing_item_id,
          closing_item_id,
          email,
        },
        null,
        2
      ),
    },
  ];
}

export function buildContractArchiveFiles({
  documents = [],
} = {}) {
  return safeArray(documents).map((document, index) => ({
    filename:
      clean(document?.name) ||
      `contract-${index + 1}.${clean(document?.file_extension || document?.extension) || "pdf"}`,
    key_name:
      clean(document?.name) ||
      `contract-${index + 1}.${clean(document?.file_extension || document?.extension) || "pdf"}`,
    content_type:
      clean(document?.content_type) ||
      `application/${clean(document?.file_extension || document?.extension || "pdf").toLowerCase()}`,
    body:
      clean(document?.file_base64) ||
      clean(document?.base64) ||
      "",
    body_encoding: "base64",
    metadata: {
      document_id: clean(document?.document_id || document?.id) || String(index + 1),
    },
  }));
}

export default {
  createStoredDocumentPackage,
  buildBuyerDispositionPackageFiles,
  buildTitleIntroPackageFiles,
  buildContractArchiveFiles,
};
