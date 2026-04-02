function createLocalTemplate({
  item_id,
  use_case,
  variant_group,
  sequence_position,
  text,
  english_translation = null,
  category_primary = "Residential",
  category_secondary = "Underwriting",
  tone = "Neutral",
  gender_variant = "Neutral",
  language = "English",
  paired_with_agent_type = "Fallback / Market-Local / Specialist-Close",
}) {
  return {
    item_id,
    raw: null,
    template_id: null,
    use_case,
    variant_group,
    tone,
    gender_variant,
    language,
    sequence_position,
    paired_with_agent_type,
    text,
    english_translation: english_translation || text,
    active: "Yes",
    is_ownership_check: "No",
    category_primary,
    category_secondary,
    personalization_tags: [],
    deliverability_score: 92,
    spam_risk: 4,
    historical_reply_rate: 24,
    total_sends: 0,
    total_replies: 0,
    total_conversations: 0,
    cooldown_days: 3,
    version: 1,
    last_used: null,
    source: "local_registry",
  };
}

export const LOCAL_TEMPLATE_CANDIDATES = Object.freeze([
  createLocalTemplate({
    item_id: "local-template:mf_units_unknown:v1",
    use_case: "mf_units_unknown",
    variant_group: "Multifamily Underwrite - Units (Open)",
    sequence_position: "V1",
    category_primary: "Landlord / Multifamily",
    text:
      "Just so I underwrite {{property_address}} correctly, how many total units are there?",
    paired_with_agent_type: "Specialist-Landlord / Market-Local",
  }),
  createLocalTemplate({
    item_id: "local-template:mf_units_unknown:v2",
    use_case: "mf_units_unknown",
    variant_group: "Multifamily Underwrite - Units (Open)",
    sequence_position: "V2",
    category_primary: "Landlord / Multifamily",
    text:
      "Quick MF underwriting check on {{property_address}}: what’s the total unit count?",
    paired_with_agent_type: "Specialist-Landlord / Market-Local",
  }),
  createLocalTemplate({
    item_id: "local-template:novation_probe:v1",
    use_case: "novation_probe",
    variant_group: "Novation Probe",
    sequence_position: "V1",
    category_secondary: "Negotiation",
    text:
      "If a straight cash number is the gap on {{property_address}}, would you be open to a novation-style option if it could net you more?",
  }),
  createLocalTemplate({
    item_id: "local-template:novation_probe:v2",
    use_case: "novation_probe",
    variant_group: "Novation Probe",
    sequence_position: "V2",
    category_secondary: "Negotiation",
    text:
      "If retail price is what matters most on {{property_address}}, would you want to hear a novation route that may improve your net?",
  }),
  createLocalTemplate({
    item_id: "local-template:novation_condition_scope:v1",
    use_case: "novation_condition_scope",
    variant_group: "Novation Underwrite - Condition Scope",
    sequence_position: "V1",
    text:
      "Before I map the best novation path on {{property_address}}, what repairs or updates would a retail buyer notice first?",
  }),
  createLocalTemplate({
    item_id: "local-template:novation_condition_scope:v2",
    use_case: "novation_condition_scope",
    variant_group: "Novation Underwrite - Condition Scope",
    sequence_position: "V2",
    text:
      "Quick condition check on {{property_address}}: what would need to be fixed, refreshed, or cleaned up before putting it in front of retail buyers?",
  }),
  createLocalTemplate({
    item_id: "local-template:novation_listing_readiness:v1",
    use_case: "novation_listing_readiness",
    variant_group: "Novation Underwrite - Listing Readiness",
    sequence_position: "V1",
    text:
      "If we took a novation route on {{property_address}}, is it vacant or show-ready enough for photos and buyer walkthroughs?",
  }),
  createLocalTemplate({
    item_id: "local-template:novation_listing_readiness:v2",
    use_case: "novation_listing_readiness",
    variant_group: "Novation Underwrite - Listing Readiness",
    sequence_position: "V2",
    text:
      "For {{property_address}}, would we have clean access for photos/showings, or is there anything that would block listing it quickly?",
  }),
  createLocalTemplate({
    item_id: "local-template:novation_timeline:v1",
    use_case: "novation_timeline",
    variant_group: "Novation Underwrite - Timeline",
    sequence_position: "V1",
    text:
      "If we aimed for a higher-net novation exit on {{property_address}}, what timeline would you be comfortable with?",
  }),
  createLocalTemplate({
    item_id: "local-template:novation_timeline:v2",
    use_case: "novation_timeline",
    variant_group: "Novation Underwrite - Timeline",
    sequence_position: "V2",
    text:
      "How quickly do you need to be done on {{property_address}} if the net to you improves with a novation approach?",
  }),
  createLocalTemplate({
    item_id: "local-template:novation_net_to_seller:v1",
    use_case: "novation_net_to_seller",
    variant_group: "Novation Underwrite - Seller Net",
    sequence_position: "V1",
    category_secondary: "Negotiation",
    text:
      "To see if a novation path is worth it on {{property_address}}, what number do you need to walk away with after fees and closing costs?",
  }),
  createLocalTemplate({
    item_id: "local-template:novation_net_to_seller:v2",
    use_case: "novation_net_to_seller",
    variant_group: "Novation Underwrite - Seller Net",
    sequence_position: "V2",
    category_secondary: "Negotiation",
    text:
      "What net amount would make {{property_address}} make sense for you if we structured it for a retail-style sale?",
  }),
  createLocalTemplate({
    item_id: "local-template:disposition_access_coordination:v1",
    use_case: "disposition_access_coordination",
    variant_group: "Disposition - Access Coordination",
    sequence_position: "V1",
    category_secondary: "Disposition",
    text:
      "We are lining up buyer access on {{property_address}}. What day and time window is easiest for photos or a quick walkthrough?",
  }),
  createLocalTemplate({
    item_id: "local-template:disposition_access_coordination:v2",
    use_case: "disposition_access_coordination",
    variant_group: "Disposition - Access Coordination",
    sequence_position: "V2",
    category_secondary: "Disposition",
    text:
      "To keep {{property_address}} moving, what access window works best for showings or buyer walkthroughs this week?",
  }),
  createLocalTemplate({
    item_id: "local-template:disposition_marketing_update:v1",
    use_case: "disposition_marketing_update",
    variant_group: "Disposition - Marketing Update",
    sequence_position: "V1",
    category_secondary: "Disposition",
    text:
      "Quick update on {{property_address}}: we are pushing the property out to active buyers now. I will keep you posted on access needs and serious interest.",
  }),
  createLocalTemplate({
    item_id: "local-template:disposition_marketing_update:v2",
    use_case: "disposition_marketing_update",
    variant_group: "Disposition - Marketing Update",
    sequence_position: "V2",
    category_secondary: "Disposition",
    text:
      "We have buyer-side marketing moving on {{property_address}} now. I will text you with any real showing activity or access needs as they come up.",
  }),
]);

export default LOCAL_TEMPLATE_CANDIDATES;
