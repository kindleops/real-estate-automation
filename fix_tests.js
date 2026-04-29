const fs = require('fs');
const files = [
  'tests/critical/replay-handlers.test.mjs',
  'tests/critical/discord-replay-command-center.test.mjs',
  'tests/critical/discord-replay-and-wires-command-center.test.mjs'
];

for (const file of files) {
  if (!fs.existsSync(file)) continue;
  let content = fs.readFileSync(file, 'utf8');

  // Replace routeSellerConversation with resolveSellerAutoReplyPlan in the mock setups
  content = content.replace(/routeSellerConversation:/g, 'resolveSellerAutoReplyPlan:');
  
  // Replace references to next_expected_stage with next_stage
  // No wait, the test actually asserts the output JSON which I modified to STILL output next_stage etc.
  // The mock just needs to return what the new planner returns:
  content = content.replace(/next_expected_stage:/g, 'next_stage:');
  content = content.replace(/detected_intent:/g, 'inbound_intent:');
  // the mock also returns "handled: true" which doesn't hurt.

  fs.writeFileSync(file, content);
}
