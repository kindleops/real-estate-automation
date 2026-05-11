
import test from "node:test";
import assert from "node:assert/strict";
import { classify } from "../../src/lib/domain/classification/classify.js";

const TEST_CASES = [
  // Ownership Confirmed
  { text: "Yes I own it", expected: "ownership_confirmed" },
  { text: "I do", expected: "ownership_confirmed" },
  { text: "Yes it's mine", expected: "ownership_confirmed" },
  { text: "Still own it. Yes.", expected: "ownership_confirmed" },
  
  // Seller Interested
  { text: "Yes and I want to sell", expected: "seller_interested" },
  { text: "I'm interested in an offer", expected: "seller_interested" },
  
  // Asking Price Provided
  { text: "150k", expected: "asking_price_provided" },
  { text: "2.1 million", expected: "asking_price_provided" },
  { text: "$500,600", expected: "asking_price_provided" },
  { text: "400,000 as is", expected: "asking_price_provided" },
  { text: "80k", expected: "asking_price_provided" },
  { text: "Half mil", expected: "asking_price_provided" },
  
  // Asks Offer
  { text: "Give me an offer", expected: "asks_offer" },
  { text: "What's your offer?", expected: "asks_offer" },
  { text: "I do. 430k cash offer", expected: "asking_price_provided" },
  
  // Condition Disclosed
  { text: "It needs a lot of work", expected: "condition_disclosed" },
  { text: "Foundation issues", expected: "condition_disclosed" },
  
  // Not Interested
  { text: "Not for sale", expected: "not_interested" },
  { text: "No thanks", expected: "not_interested" },
  { text: "I'm not interested in selling.", expected: "not_interested" },
  { text: "Yes but not for sale.", expected: "not_interested" },
  { text: "Answer is no", expected: "not_interested" },
  { text: "I'm not looking to sell.", expected: "not_interested" },
  { text: "It is mine. I am not selling it.", expected: "not_interested" },
  { text: "Not selling", expected: "not_interested" },
  { text: "I am a real estate agent", expected: "not_interested" },
  
  // Maybe Later / Need Time
  { text: "Maybe in a few months", expected: "need_time" },
  { text: "No, not at this time. Maybe someday down the road.", expected: "need_time" },
  
  // Wrong Number
  { text: "Wrong number", expected: "wrong_number" },
  { text: "You have the wrong person", expected: "wrong_number" },
  { text: "I don't own that house", expected: "wrong_number" },
  { text: "Sold it 10 yrs ago", expected: "wrong_number" },
  { text: "Sold it last week for $80,000!", expected: "wrong_number" },
  { text: "This is not Shirley...", expected: "wrong_number" },
  { text: "No It sold", expected: "wrong_number" },
  { text: "No la Mia es 2711 Degen Dr. Bonita CA 91902", expected: "wrong_number" },
  { text: "esa. Casa. llanoesmia", expected: "wrong_number" },
  
  // Opt Out
  { text: "Stop", expected: "opt_out" },
  { text: "Remove me from your list", expected: "opt_out" },
  { text: "Please get me off that list", expected: "opt_out" },
  { text: "Don't bother me again", expected: "opt_out" },
  { text: "Buzz off", expected: "opt_out" },
  { text: "Spam", expected: "opt_out" },
  { text: "Congratulations I'm now looking into legal action Stop harassing me", expected: "opt_out" },
  { text: "Yes and never contact me again", expected: "opt_out" },
  
  // Who is this
  { text: "Who is this?", expected: "who_is_this" },
  { text: "Who be this", expected: "who_is_this" },
  { text: "How did you get my number?", expected: "who_is_this" },

  // Spanish
  { text: "Si cual alejandro", expected: "ownership_confirmed" },
  { text: "No elimíname de tu lista", expected: "opt_out" },
  { text: "Si pero No está en venta", expected: "not_interested" },
];

test("Classification Regression Suite", async (t) => {
  for (const tc of TEST_CASES) {
    await t.test(`should classify "${tc.text}" as ${tc.expected}`, async () => {
      const result = await classify(tc.text);
      assert.strictEqual(result.primary_intent, tc.expected, `Failed for: "${tc.text}"`);
    });
  }
});
