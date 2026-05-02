import test from "node:test";
import assert from "node:assert/strict";
import {
  scorePropertyTypeScopeMatch,
  describePropertyTypeScopeCompatibility,
} from "../../src/lib/domain/templates/template-selector.js";

test("Unit Count Guards: 5+ Units template is rejected for Duplex property", () => {
  const result = describePropertyTypeScopeCompatibility({
    requested_property_type_scope: "Duplex",
    template_property_type_scope: "5+ Units",
  });
  assert.equal(result.compatible, false);
  assert.equal(result.reason, "unit_specific_template_mismatch");

  const score = scorePropertyTypeScopeMatch({
    requested_property_type_scope: "Duplex",
    template_property_type_scope: "5+ Units",
  });
  assert.equal(score, 0);
});

test("Unit Count Guards: Duplex template is rejected for 5+ Units property", () => {
  const result = describePropertyTypeScopeCompatibility({
    requested_property_type_scope: "5+ Units",
    template_property_type_scope: "Duplex",
  });
  assert.equal(result.compatible, false);
  assert.equal(result.reason, "unit_specific_template_mismatch");

  const score = scorePropertyTypeScopeMatch({
    requested_property_type_scope: "5+ Units",
    template_property_type_scope: "Duplex",
  });
  assert.equal(score, 0);
});

test("Unit Count Guards: 5+ Units template is rejected for Single Family property", () => {
  const result = describePropertyTypeScopeCompatibility({
    requested_property_type_scope: "Residential",
    template_property_type_scope: "5+ Units",
  });
  assert.equal(result.compatible, false);
  // describePropertyTypeScopeCompatibility for Residential requested vs Multifamily template
  // currently returns residential_scope_rejected_multifamily_only (line 674)
  assert.equal(result.reason, "residential_scope_rejected_multifamily_only");

  const score = scorePropertyTypeScopeMatch({
    requested_property_type_scope: "Residential",
    template_property_type_scope: "5+ Units",
  });
  assert.equal(score, 0);
});

test("Unit Count Guards: Generic Multifamily template is allowed for Duplex (fallback)", () => {
  const result = describePropertyTypeScopeCompatibility({
    requested_property_type_scope: "Duplex",
    template_property_type_scope: "Landlord / Multifamily",
  });
  assert.equal(result.compatible, true);
  assert.equal(result.reason, "multifamily_scope_family_match");

  const score = scorePropertyTypeScopeMatch({
    requested_property_type_scope: "Duplex",
    template_property_type_scope: "Landlord / Multifamily",
  });
  assert.equal(score, 85);
});

test("Unit Count Guards: Unit-specific template is rejected for unknown unit multifamily", () => {
  const result = describePropertyTypeScopeCompatibility({
    requested_property_type_scope: "Landlord / Multifamily", // Unknown units
    template_property_type_scope: "Duplex",
  });
  // Since requested is NOT unit-specific, it falls into the generic isMultifamilyScope(requested) block
  // Our new check: if (isUnitSpecificScope(template_scope) && !safeCategoryEquals(requested, template_scope))
  assert.equal(result.compatible, false);
  assert.equal(result.reason, "unit_specific_template_mismatch");

  const score = scorePropertyTypeScopeMatch({
    requested_property_type_scope: "Landlord / Multifamily",
    template_property_type_scope: "Duplex",
  });
  assert.equal(score, 0);
});

test("Unit Count Guards: Exact match still works for unit-specific scopes", () => {
  const result = describePropertyTypeScopeCompatibility({
    requested_property_type_scope: "Fourplex",
    template_property_type_scope: "Fourplex",
  });
  assert.equal(result.compatible, true);
  assert.equal(result.reason, "exact_scope_match");

  const score = scorePropertyTypeScopeMatch({
    requested_property_type_scope: "Fourplex",
    template_property_type_scope: "Fourplex",
  });
  assert.equal(score, 100);
});
