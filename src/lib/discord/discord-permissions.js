/**
 * discord-permissions.js
 *
 * Role-based access control for Discord slash commands and button interactions.
 *
 * Role IDs are read from environment variables at call time (not module load
 * time) so they work correctly across test and production environments.
 *
 * Permission hierarchy (each role implicitly includes the roles below it):
 *   Owner        → all commands
 *   Tech Ops     → queue run, sync podio, diagnostics, lock release, feeder ≤ 25
 *   SMS Ops      → campaign pause (resume requires Owner)
 *   Acquisitions → lead summarize (read-only)
 *   Closings     → no elevated commands (informational access only)
 */

// ---------------------------------------------------------------------------
// Role resolver
// ---------------------------------------------------------------------------

const ROLE_ENV_MAP = {
  owner:        "DISCORD_ROLE_OWNER_ID",
  tech_ops:     "DISCORD_ROLE_TECH_OPS_ID",
  sms_ops:      "DISCORD_ROLE_SMS_OPS_ID",
  acquisitions: "DISCORD_ROLE_ACQUISITIONS_ID",
  closings:     "DISCORD_ROLE_CLOSINGS_ID",
};

/**
 * Return the configured role ID for a named role, or null if unconfigured.
 * @param {string} role_name
 * @returns {string|null}
 */
function getRoleId(role_name) {
  const env_key = ROLE_ENV_MAP[role_name];
  if (!env_key) return null;
  const value = String(process.env[env_key] ?? "").trim();
  return value || null;
}

// ---------------------------------------------------------------------------
// Core permission checks
// ---------------------------------------------------------------------------

/**
 * Check whether the member's role_ids array contains the Discord role ID
 * that corresponds to role_name.
 *
 * @param {string[]} member_role_ids  - array of Discord role ID strings from interaction.member.roles
 * @param {string}   role_name        - one of: "owner", "tech_ops", "sms_ops", "acquisitions", "closings"
 * @returns {boolean}
 */
export function hasRole(member_role_ids, role_name) {
  const role_id = getRoleId(role_name);
  if (!role_id) return false;
  return Array.isArray(member_role_ids) && member_role_ids.includes(role_id);
}

export function isOwner(member_role_ids)        { return hasRole(member_role_ids, "owner"); }
export function isTechOps(member_role_ids)      { return hasRole(member_role_ids, "tech_ops") || isOwner(member_role_ids); }
export function isSmsOps(member_role_ids)       { return hasRole(member_role_ids, "sms_ops")  || isOwner(member_role_ids); }
export function isAcquisitions(member_role_ids) { return hasRole(member_role_ids, "acquisitions") || isOwner(member_role_ids); }

/**
 * Check whether the member has ANY of the listed required roles.
 *
 * @param {string[]} member_role_ids
 * @param {string[]} required_roles  - e.g. ["owner", "tech_ops"]
 * @returns {boolean}
 */
export function checkPermission(member_role_ids, required_roles) {
  return required_roles.some((role) => {
    switch (role) {
      case "owner":        return isOwner(member_role_ids);
      case "tech_ops":     return isTechOps(member_role_ids);
      case "sms_ops":      return isSmsOps(member_role_ids);
      case "acquisitions": return isAcquisitions(member_role_ids);
      default:             return hasRole(member_role_ids, role);
    }
  });
}

// ---------------------------------------------------------------------------
// Context extractor
// ---------------------------------------------------------------------------

/**
 * Extract a normalised member context from a Discord interaction object.
 *
 * @param {object} interaction - Raw Discord interaction body.
 * @returns {{ user_id, username, guild_id, channel_id, role_ids }}
 */
export function extractMemberContext(interaction) {
  const member = interaction?.member;
  const user   = member?.user ?? interaction?.user ?? {};
  return {
    user_id:    String(user?.id    ?? ""),
    username:   String(user?.username ?? ""),
    guild_id:   String(interaction?.guild_id  ?? ""),
    channel_id: String(interaction?.channel_id ?? ""),
    role_ids:   Array.isArray(member?.roles) ? member.roles : [],
  };
}

// ---------------------------------------------------------------------------
// Role mention helpers
// ---------------------------------------------------------------------------

/**
 * Collect all configured role IDs that are referenced by name in the list.
 * Returns only non-empty IDs so callers don't need to guard against nulls.
 *
 * @param {string[]} role_names
 * @returns {string[]}
 */
export function resolveRoleIds(role_names) {
  return role_names.map(getRoleId).filter(Boolean);
}
