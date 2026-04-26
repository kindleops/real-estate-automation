import supabase from "@/lib/supabase/client.js";

export function getDefaultSupabaseClient() {
  return globalThis.__rea_default_supabase_client__ || supabase;
}

export default getDefaultSupabaseClient;