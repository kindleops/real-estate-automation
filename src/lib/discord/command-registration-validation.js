export function validateCommandOptionCounts(commands, max_options = 25) {
  const walk = (node, command_name = null, subcommand_name = null) => {
    if (!node || typeof node !== "object") return;

    const current_command = command_name ?? node.name ?? null;
    const is_subcommand = node.type === 1;
    const current_subcommand = is_subcommand ? (node.name ?? subcommand_name) : subcommand_name;

    if (Array.isArray(node.options)) {
      if (node.options.length > max_options) {
        const option_names = node.options.map((opt) => String(opt?.name ?? "(unnamed)"));
        throw new Error(
          [
            "Discord command options exceed maximum allowed length.",
            `command: ${current_command ?? "(unknown)"}`,
            `subcommand: ${current_subcommand ?? "(none)"}`,
            `option_count: ${node.options.length}`,
            `option_names: ${option_names.join(", ")}`,
          ].join("\n")
        );
      }

      for (const child of node.options) {
        walk(child, current_command, current_subcommand);
      }
    }
  };

  for (const cmd of commands) {
    walk(cmd, cmd?.name ?? null, null);
  }
}

export function validateCommandPayloadSizes(commands, max_bytes = 8000) {
  for (const command of commands ?? []) {
    const payload = JSON.stringify(command ?? {});
    const payload_length = payload.length;

    if (payload_length >= max_bytes) {
      const option_names = Array.isArray(command?.options)
        ? command.options.map((opt) => String(opt?.name ?? "(unnamed)"))
        : [];

      throw new Error(
        [
          "Discord command payload exceeds maximum allowed size.",
          `command: ${String(command?.name ?? "(unknown)")}`,
          `json_length: ${payload_length}`,
          `max_allowed: ${max_bytes}`,
          `top_level_option_names: ${option_names.join(", ")}`,
        ].join("\n")
      );
    }
  }
}
