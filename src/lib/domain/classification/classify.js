// ─── classify.js ──────────────────────────────────────────────────────────
import { ai } from "@/lib/providers/ai.js";
import {
  getCategoryValue,
  normalizeLanguage,
  normalizeStage,
} from "@/lib/providers/podio.js";

// ══════════════════════════════════════════════════════════════════════════
// UTILITIES
// ══════════════════════════════════════════════════════════════════════════

function cleanMessage(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return cleanMessage(value).toLowerCase();
}

function includesAny(text, phrases = []) {
  return phrases.some((p) => text.includes(p));
}

function wordCount(text) {
  return text.trim().split(/\s+/).filter(Boolean).length;
}

function sanitizeForPrompt(message) {
  return message
    .replace(/`/g, "'")
    .replace(/\$/g, "")
    .replace(/[<>]/g, "")
    .replace(/\{\{/g, "")
    .replace(/\}\}/g, "")
    .slice(0, 800);
}

// ══════════════════════════════════════════════════════════════════════════
// LANGUAGE DETECTION
// ══════════════════════════════════════════════════════════════════════════

/**
 * IMPORTANT: Script-based detection runs first (no false positives).
 * Keyword-based detection runs second (Latin-script languages only).
 * Fall through to brain preference → "English" as final default.
 *
 * Per-language keywords cover:
 *   greetings, ownership, sale intent, disinterest, tenant situations,
 *   wrong-number, price curiosity, compliance opt-outs, and common replies.
 */
const LANGUAGE_PATTERNS = [
  // ── Script-detected (non-Latin) ─────────────────────────────────────────
  { language: "Hebrew",    script: /[\u0590-\u05FF]/, keywords: [] },
  { language: "Mandarin",  script: /[\u4E00-\u9FFF]/, keywords: [] },
  { language: "Korean",    script: /[\uAC00-\uD7AF]/, keywords: [] },
  { language: "Japanese",  script: /[\u3040-\u30FF]/, keywords: [] },
  { language: "Arabic",    script: /[\u0600-\u06FF]/, keywords: [] },
  { language: "Russian",   script: /[\u0400-\u04FF]/, keywords: [] },
  { language: "Thai",      script: /[\u0E00-\u0E7F]/, keywords: [] },
  { language: "Hindi",     script: /[\u0900-\u097F]/, keywords: [] },
  { language: "Greek",     script: /[\u0370-\u03FF]/, keywords: [] },

  // ── Keyword-detected (Latin-script) ─────────────────────────────────────
  {
    language: "Spanish",
    script: null,
    keywords: [
      // Greetings & general
      "hola", "buenas", "buenos días", "buenas tardes", "buenas noches",
      "buen día", "qué tal", "cómo estás",
      // Ownership / property
      "propiedad", "propiedades", "el dueño", "la dueña", "soy el dueño",
      "soy dueño", "soy la dueña", "soy dueña", "el propietario",
      "la propietaria", "soy el propietario", "soy propietario",
      // Sale intent
      "quiero vender", "vender", "vendo", "podría vender", "puedo vender",
      "estoy interesado", "me interesa", "precio", "cuánto ofrecen",
      "cuánto me dan", "cuánto pagan", "cuánto paga", "cuánto es",
      "me llamo", "necesito vender", "interesa vender",
      // Disinterest
      "no me interesa", "no quiero vender", "no está en venta",
      "no lo voy a vender", "no estoy interesado",
      // Tenant / property status
      "está rentada", "está ocupada", "tengo inquilinos", "inquilino",
      "arrendatario", "rentada", "rentado",
      // Wrong number / already sold
      "número equivocado", "no soy el dueño", "ya lo vendí",
      "ya lo vendi", "no tengo esa propiedad", "no es mía",
      "no es mia", "no soy el propietario",
      // Time / family
      "necesito tiempo", "no estoy listo", "hablar con mi",
      "consultar con mi", "necesito consultar",
      // Compliance
      "no me escribas", "no me contactes", "quítame de tu lista",
      "quitame de tu lista", "no me mandes mensajes",
      "para de escribirme", "no quiero mensajes",
      "no me llames", "para de contactarme", "párate",
      "detente", "basta", "ya no me llames",
      // Misc affirmations
      "gracias", "por favor", "sí señor", "sí señora",
      "entendido", "de acuerdo", "está bien", "claro",
      "por supuesto", "sale", "órale", "ándale",
    ],
  },
  {
    language: "Portuguese",
    script: null,
    keywords: [
      // Greetings
      "olá", "oi tudo", "oi", "bom dia", "boa tarde", "boa noite",
      "tudo bem", "tudo bom",
      // Ownership / property
      "imóvel", "imovel", "proprietário", "proprietario", "sou o proprietário",
      "sou proprietário", "sou a proprietária",
      // Sale intent
      "quero vender", "valor", "você", "quanto você paga",
      "quanto paga", "quanto oferecem", "me chamo",
      "tenho interesse", "estou interessado",
      // Disinterest
      "não tenho interesse", "nao tenho interesse", "não quero vender",
      "nao quero vender", "não está à venda", "nao esta a venda",
      "não vou vender", "vou manter",
      // Tenant
      "está alugado", "esta alugado", "inquilino", "locatário",
      "locatario", "alugado",
      // Wrong number / sold
      "número errado", "numero errado", "não sou o proprietário",
      "nao sou o proprietario", "já vendi", "ja vendi",
      "não tenho esse imóvel", "nao tenho esse imovel",
      // Time / family
      "preciso de tempo", "não estou pronto", "nao estou pronto",
      "mais para frente", "falar com minha", "falar com meu",
      "preciso consultar",
      // Compliance
      "pare de me mandar mensagem", "não me contate", "nao me contate",
      "me remove da lista", "não me mande mensagem",
      "nao me mande mensagem", "pare de me ligar",
      "não me contate mais", "nao me contate mais",
      // Affirmations
      "obrigado", "obrigada", "pode ser", "entendido",
      "tudo certo", "beleza", "com certeza", "sim senhor",
    ],
  },
  {
    language: "Italian",
    script: null,
    keywords: [
      // Greetings
      "ciao", "salve", "buongiorno", "buonasera", "buona sera",
      "buon giorno",
      // Ownership / property
      "immobile", "proprietario", "proprietaria",
      "sono il proprietario", "sono la proprietaria",
      "sono proprietario",
      // Sale intent
      "vendere", "quanto offri", "quanto paghi",
      "prezzo", "offerta", "mi chiamo",
      "sono interessato", "sono interessata",
      // Disinterest
      "non sono interessato", "non sono interessata",
      "non voglio vendere", "non è in vendita",
      "non la venderò",
      // Tenant
      "affittato", "affittata", "locatario", "inquilino",
      "inquilina", "in affitto",
      // Wrong number / sold
      "numero sbagliato", "non sono il proprietario",
      "non sono la proprietaria", "l'ho già venduto",
      "l'ho gia venduto", "non è mia", "non e mia",
      // Time / family
      "ho bisogno di tempo", "non sono pronto", "non sono pronta",
      "più avanti", "piu avanti", "parlare con mia moglie",
      "parlare con mio marito", "parlare con la famiglia",
      "devo consultare",
      // Compliance
      "non contattarmi più", "non contattarmi piu",
      "smettila di scrivermi", "toglimi dalla lista",
      "non mandarmi messaggi", "smettila di chiamarmi",
      "basta messaggi",
      // Affirmations
      "grazie", "capito", "va bene", "certo",
      "certamente", "d'accordo", "sì", "ok",
    ],
  },
  {
    language: "French",
    script: null,
    keywords: [
      // Greetings
      "bonjour", "bonsoir", "salut", "allô", "allo",
      "bonne journée", "bonne soirée",
      // Ownership / property
      "propriété", "propriete", "propriétaire", "proprietaire",
      "je suis le propriétaire", "je suis propriétaire",
      // Sale intent
      "vendre", "combien", "vous payez combien", "offre", "prix",
      "je suis intéressé", "je suis interessé",
      // Disinterest
      "pas intéressé", "pas interesse", "je ne veux pas vendre",
      "pas à vendre", "pas a vendre", "je vais garder",
      // Tenant
      "loué", "loue", "locataire", "en location",
      // Wrong number / sold
      "mauvais numéro", "mauvais numero", "je ne suis pas le propriétaire",
      "je l'ai déjà vendu", "je l'ai deja vendu",
      // Time / family
      "j'ai besoin de temps", "pas encore prêt",
      "parler avec ma femme", "parler avec mon mari",
      "consulter ma famille",
      // Compliance
      "arrête de m'écrire", "arrete de m'ecrire",
      "ne me contacte plus", "retire-moi de ta liste",
      "retirez-moi de votre liste", "arrêtez de m'envoyer",
      "ne m'écrivez plus", "cessez de me contacter",
      // Affirmations
      "merci", "d'accord", "bien sûr", "bien sur",
      "entendu", "compris", "oui",
    ],
  },
  {
    language: "German",
    script: null,
    keywords: [
      // Greetings
      "hallo", "guten tag", "guten morgen", "guten abend",
      "gute nacht", "servus", "moin",
      // Ownership / property
      "immobilie", "eigentümer", "eigentuemer", "ich bin der eigentümer",
      "ich bin eigentümer",
      // Sale intent
      "verkaufen", "wie viel", "wieviel", "was zahlen sie",
      "ich bin interessiert", "angebot",
      // Disinterest
      "nicht interessiert", "ich möchte nicht verkaufen",
      "nicht zu verkaufen", "ich behalte es",
      // Tenant
      "vermietet", "mieter", "in miete",
      // Wrong number / sold
      "falsche nummer", "ich bin nicht der eigentümer",
      "schon verkauft", "habe es verkauft",
      // Time / family
      "brauche zeit", "noch nicht bereit",
      "mit meiner frau besprechen", "mit meinem mann besprechen",
      "mit der familie besprechen",
      // Compliance
      "hör auf mir zu schreiben", "kontaktiere mich nicht mehr",
      "entferne mich von deiner liste",
      "schreib mir nicht mehr", "ruf mich nicht an",
      "keine nachrichten mehr", "aufhören",
      // Affirmations
      "danke", "verstanden", "ok", "gut", "in ordnung",
      "natürlich", "ja",
    ],
  },
  {
    language: "Vietnamese",
    script: null,
    keywords: [
      // Greetings
      "xin chào", "chào bạn", "chào anh", "chào chị",
      "kính chào",
      // Ownership / property
      "chủ nhà", "chủ sở hữu", "tôi là chủ", "tôi là chủ nhà",
      "tôi là chủ sở hữu",
      // Sale intent
      "bán nhà", "tôi muốn bán", "muốn bán",
      "giá bao nhiêu", "bao nhiêu tiền",
      "trả bao nhiêu", "tôi quan tâm",
      // Disinterest
      "không quan tâm", "không muốn bán",
      "không bán", "giữ lại",
      // Tenant
      "đang cho thuê", "thuê nhà", "người thuê",
      // Wrong number / sold
      "số sai", "nhầm số", "không phải chủ",
      "đã bán rồi", "đã bán",
      // Time / family
      "cần thêm thời gian", "chưa sẵn sàng",
      "hỏi ý kiến vợ", "hỏi ý kiến chồng",
      "bàn với gia đình",
      // Compliance
      "đừng nhắn tin", "dừng nhắn tin",
      "xóa số của tôi", "không liên lạc nữa",
      // Affirmations
      "cảm ơn", "được rồi", "hiểu rồi",
      "vâng", "đúng rồi",
    ],
  },
  {
    language: "Polish",
    script: null,
    keywords: [
      // Greetings
      "dzień dobry", "dobry wieczór", "cześć", "hej", "siema",
      // Ownership / property
      "nieruchomość", "właściciel", "właścicielka",
      "jestem właścicielem", "jestem właścicielką",
      // Sale intent
      "chcę sprzedać", "sprzedać", "ile płacicie",
      "ile oferujecie", "oferta", "cena", "jestem zainteresowany",
      // Disinterest
      "nie jestem zainteresowany", "nie chcę sprzedawać",
      "nie na sprzedaż", "zatrzymam to",
      // Tenant
      "wynajmowane", "lokator", "lokatorka", "w wynajmie",
      // Wrong number / sold
      "zły numer", "nie jestem właścicielem",
      "już sprzedałem", "już sprzedałam",
      // Time / family
      "potrzebuję czasu", "nie jestem gotowy",
      "porozmawiać z żoną", "porozmawiać z mężem",
      "skonsultować z rodziną",
      // Compliance
      "przestań pisać", "nie kontaktuj się ze mną",
      "usuń mnie z listy", "nie chcę wiadomości",
      // Affirmations
      "dziękuję", "rozumiem", "okej", "dobrze",
      "jasne", "tak",
    ],
  },
];

function detectLanguageHeuristic(message, brain_item = null) {
  const text = lower(message);
  const brain_language = normalizeLanguage(
    getCategoryValue(brain_item, "language-preference", "English")
  );

  for (const pattern of LANGUAGE_PATTERNS) {
    if (pattern.script?.test(message)) return pattern.language;
    if (pattern.keywords.length > 0 && includesAny(text, pattern.keywords)) {
      return pattern.language;
    }
  }

  return brain_language || "English";
}

// ══════════════════════════════════════════════════════════════════════════
// COMPLIANCE DETECTION
// ══════════════════════════════════════════════════════════════════════════

/**
 * TCPA 2025/2026: "any reasonable means" standard — keyword-exact AND
 * phrase-intent must both be covered. Multilingual opt-outs are legally
 * binding and must be honored immediately.
 *
 * Carrier standard keywords: STOP, END, CANCEL, QUIT, UNSUBSCRIBE, STOPALL.
 * These must be recognized as an exact message or at the start of a message.
 */
const COMPLIANCE_EXACT = new Set([
  "stop", "end", "cancel", "quit", "unsubscribe", "stopall",
  "opt out", "optout", "opt-out",
  // Spanish
  "para", "detente", "basta", "cancela",
  // Portuguese
  "parar", "cancelar", "sair",
  // Italian
  "ferma", "fermati", "cancella",
  // French
  "arrêt", "arret", "annuler",
  // German
  "stopp", "aufhören", "abmelden",
  // Vietnamese
  "dừng", "dung",
  // Polish
  "zatrzymaj", "odpisz",
]);

const COMPLIANCE_PHRASES = [
  // ── English ──────────────────────────────────────────────────────────────
  "stop texting", "stop texting me", "stop messaging", "stop messaging me",
  "stop contacting", "stop contacting me", "stop calling me",
  "stop calling", "stop all messages", "stop all texts",
  "quit texting", "quit texting me", "quit messaging",
  "quit messaging me", "quit contacting me",
  "quit calling me",
  "remove me", "remove my number", "remove me from your list",
  "remove me from your database", "remove my number from your list",
  "take me off", "take me off your list", "take me off this list",
  "take my number off", "take my number off your list",
  "delete my number", "delete me from your list",
  "do not text", "do not text me", "do not contact",
  "do not contact me", "do not message", "do not message me",
  "do not call", "do not call me", "do not reach out",
  "don't text me", "dont text me", "don't message me",
  "dont message me", "don't contact me", "dont contact me",
  "don't call me", "dont call me",
  "never text me", "never call me", "never contact me",
  "never message me", "never reach out",
  "please don't contact me", "please do not contact me",
  "please stop texting", "please stop messaging",
  "please remove me", "please take me off",
  "opt out", "opt me out", "i want to opt out",
  "i said stop", "i already said stop",
  "told you to stop", "told you not to text",
  "asked you to stop", "asked you not to text",
  "leave me alone", "leave me the hell alone",
  "leave me the f*** alone", "just leave me alone",
  "report you", "report this number", "report as spam",
  "i'll report you", "going to report you", "going to report this",
  "reporting this number",
  "go away", "get lost", "get out of here",
  "this is harassment", "stop harassing me",
  "you're harassing me", "this is spam",
  "how dare you", "this is illegal",
  "add me to your dnc", "add me to do not call",
  "put me on your dnc", "add me to the do not call list",
  "not interested stop", "no interest stop",
  "i don't want your texts", "i dont want your texts",
  "i don't want to hear from you", "dont want to hear from you",
  "i don't want messages from you",

  // ── Spanish ───────────────────────────────────────────────────────────────
  "no me escribas", "no me escribas más", "no me escribas mas",
  "no me contactes", "no me contactes más", "no me contactes mas",
  "no me mandes mensajes", "no me mandes más mensajes",
  "para de escribirme", "para de contactarme",
  "deja de escribirme", "deja de contactarme",
  "deja de mandarme mensajes", "deja de llamarme",
  "quítame de tu lista", "quitame de tu lista",
  "bórrame de tu lista", "borrame de tu lista",
  "elimina mi número", "elimina mi numero",
  "no quiero mensajes", "no quiero más mensajes",
  "no me llames", "no vuelvas a llamarme",
  "me tienes harto", "me tienes harta",
  "esto es acoso", "me estás acosando",
  "voy a reportarte", "te voy a reportar",
  "agrégame al no llamar", "ponme en la lista de no llamar",
  "ya te dije que pares", "ya te dije que no me contactes",
  "párate", "ya basta", "suficiente",

  // ── Portuguese ────────────────────────────────────────────────────────────
  "pare de me mandar mensagem", "pare de me mandar mensagens",
  "não me contate", "nao me contate",
  "não me contate mais", "nao me contate mais",
  "me remove da lista", "remova meu número da lista",
  "remova meu numero da lista",
  "não me mande mensagem", "nao me mande mensagem",
  "não me mande mais mensagens", "nao me mande mais mensagens",
  "pare de me ligar", "não me ligue",
  "não me ligue mais", "nao me ligue mais",
  "não me contate de novo", "nao me contate de novo",
  "deixe-me em paz", "me deixa em paz",
  "isso é assédio", "isso e assedio",
  "vou te denunciar", "vou reportar esse número",
  "me adiciona ao não ligue", "adicione ao não perturbe",

  // ── Italian ───────────────────────────────────────────────────────────────
  "non contattarmi più", "non contattarmi piu",
  "non contattarmi più mai", "smettila di scrivermi",
  "smettila di mandarmi messaggi", "smettila di chiamarmi",
  "toglimi dalla lista", "rimuovi il mio numero",
  "rimuovi il mio numero dalla lista",
  "non mandarmi messaggi", "non mandarmi più messaggi",
  "non chiamarmi", "non chiamarmi più",
  "lasciami in pace", "questo è stalking",
  "questo è molestia", "ti denuncio",
  "non voglio essere contattato", "non voglio essere contattata",
  "basta messaggi", "non voglio più sentirti",

  // ── French ────────────────────────────────────────────────────────────────
  "arrête de m'écrire", "arrete de m'ecrire",
  "arrêtez de m'écrire", "arretez de m'ecrire",
  "ne me contacte plus", "ne me contactez plus",
  "retire-moi de ta liste", "retirez-moi de votre liste",
  "supprime mon numéro", "supprimez mon numéro",
  "ne m'écrivez plus", "ne m'ecrivez plus",
  "ne m'envoie plus de messages", "n'essaie plus de me contacter",
  "laisse-moi tranquille", "laissez-moi tranquille",
  "c'est du harcèlement", "c'est du harcelement",
  "je vais vous signaler", "je vais vous dénoncer",
  "ajoutez-moi à la liste ne pas appeler",
  "arrêtez de m'appeler", "ne m'appelez plus",

  // ── German ────────────────────────────────────────────────────────────────
  "hör auf mir zu schreiben", "horen sie auf mir zu schreiben",
  "kontaktiere mich nicht mehr", "kontaktieren sie mich nicht mehr",
  "entferne mich von deiner liste", "entfernen sie mich von ihrer liste",
  "lösch meine nummer", "löschen sie meine nummer",
  "schreib mir nicht mehr", "schreiben sie mir nicht mehr",
  "ruf mich nicht an", "rufen sie mich nicht an",
  "keine nachrichten mehr", "keine sms mehr",
  "lass mich in ruhe", "lassen sie mich in ruhe",
  "das ist belästigung", "das ist harassment",
  "ich werde sie melden", "ich melde diese nummer",
  "auf die sperrliste", "zu meiner do-not-call-liste",

  // ── Vietnamese ────────────────────────────────────────────────────────────
  "đừng nhắn tin", "dừng nhắn tin",
  "không liên lạc nữa", "xóa số của tôi",
  "xóa số tôi khỏi danh sách", "đừng gọi cho tôi",
  "đừng nhắn tin cho tôi nữa", "tôi không muốn nghe",
  "để tôi yên", "thôi đi", "bỏ số tôi ra",

  // ── Polish ────────────────────────────────────────────────────────────────
  "przestań pisać", "przestań się kontaktować",
  "usuń mnie z listy", "usuń mój numer",
  "nie kontaktuj się ze mną", "nie piszę mi",
  "nie dzwoń do mnie", "zostaw mnie w spokoju",
  "to jest nękanie", "zgłoszę ten numer",
  "nie chcę wiadomości", "nie chcę być kontaktowany",
];

function detectComplianceFlag(message) {
  const text  = lower(message);
  const trimmed = text.trim();

  // Exact keyword match (entire message is the keyword)
  if (COMPLIANCE_EXACT.has(trimmed)) return "stop_texting";

  // Message starts with a carrier keyword (e.g., "STOP please")
  for (const kw of COMPLIANCE_EXACT) {
    if (trimmed.startsWith(kw + " ") || trimmed.startsWith(kw + ",")) {
      return "stop_texting";
    }
  }

  // Phrase-intent match
  if (includesAny(text, COMPLIANCE_PHRASES)) return "stop_texting";

  return null;
}

// ══════════════════════════════════════════════════════════════════════════
// OBJECTION DETECTION
// ══════════════════════════════════════════════════════════════════════════

const OBJECTION_MAP = [
  {
    key: "wrong_number",
    phrases: [
      // English
      "wrong number", "you have the wrong number", "wrong person",
      "not the owner", "not the property owner", "not the homeowner",
      "doesn't own", "don't own", "no longer own", "i no longer own",
      "i sold it", "already sold", "sold that property", "sold that house",
      "sold years ago", "sold it years ago", "sold long ago",
      "don't know this person", "not associated with",
      "no longer my number", "this isn't my property",
      "not mine", "not my property", "i don't own", "i don't own that",
      "i don't own this", "that's not my property",
      "that is not my property", "incorrect number",
      // Spanish
      "número equivocado", "equivocado de número",
      "no soy el dueño", "no soy la dueña",
      "no soy el propietario", "no soy la propietaria",
      "ya lo vendí", "ya lo vendi", "no tengo esa propiedad",
      "no es mía", "no es mia", "no es mi propiedad",
      "no tengo propiedades", "yo no tengo esa propiedad",
      // Portuguese
      "número errado", "numero errado", "não sou o proprietário",
      "nao sou o proprietario", "não sou a proprietária",
      "já vendi", "ja vendi", "não tenho esse imóvel",
      "nao tenho esse imovel", "não é meu", "nao e meu",
      "não é minha propriedade",
      // Italian
      "numero sbagliato", "non sono il proprietario",
      "non sono la proprietaria", "l'ho già venduto",
      "l'ho gia venduto", "non è mia", "non e mia",
      "non è di mia proprietà",
      // French
      "mauvais numéro", "mauvais numero", "je ne suis pas le propriétaire",
      "je ne suis pas la propriétaire", "je l'ai déjà vendu",
      "ce n'est pas ma propriété", "ce n'est pas le bon numéro",
      // German
      "falsche nummer", "ich bin nicht der eigentümer",
      "ich bin nicht die eigentümerin", "habe es schon verkauft",
      "schon verkauft", "das gehört mir nicht",
      // Vietnamese
      "số sai", "nhầm số", "không phải chủ",
      "đã bán rồi", "đây không phải nhà tôi",
      // Polish
      "zły numer", "nie jestem właścicielem",
      "nie jestem właścicielką", "już sprzedałem",
      "już sprzedałam", "to nie moja nieruchomość",
    ],
  },
  {
    key: "who_is_this",
    phrases: [
      // English
      "who is this", "whos this", "who's this", "who are you",
      "who am i speaking to", "what company is this",
      "how did you get my number", "where did you get my number",
      "how did you find my number", "how you got my number",
      "why are you texting me", "why are you contacting me",
      "what is this about", "what's this about", "what's this",
      "are you legit", "is this legit", "is this real",
      "is this a scam", "what is this", "not sure who this is",
      "identify yourself", "who gave you my number",
      "i don't know you", "i don't recognize this number",
      "i don't recognize this", "not a number i know",
      "who sent this", "who's messaging me",
      // Spanish
      "quién eres", "quién es usted", "cómo tienes mi número",
      "de dónde sacaste mi número", "de donde sacaste mi numero",
      "qué es esto", "que es esto", "quién me escribe",
      "quién me está escribiendo", "no reconozco este número",
      "de qué empresa son",
      // Portuguese
      "quem é você", "quem e voce", "como conseguiu meu número",
      "como conseguiu meu numero", "o que é isso", "o que e isso",
      "não reconheço esse número", "nao reconheco esse numero",
      "de qual empresa é", "quem está me enviando",
      // Italian
      "chi sei", "chi è lei", "come hai avuto il mio numero",
      "come ha avuto il mio numero",
      "cos'è questo", "non conosco questo numero",
      "di quale azienda sei", "chi mi sta scrivendo",
      // French
      "qui êtes-vous", "qui etes-vous", "comment avez-vous eu mon numéro",
      "d'où vient ce numéro", "je ne connais pas ce numéro",
      "de quelle entreprise êtes-vous",
      // German
      "wer sind sie", "woher haben sie meine nummer",
      "was ist das", "ich kenne diese nummer nicht",
      "welche firma sind sie",
      // Vietnamese
      "ai vậy", "bạn là ai", "sao có số tôi",
      "đây là ai", "số này của ai",
      // Polish
      "kim jesteś", "skąd masz mój numer",
      "co to jest", "nie znam tego numeru",
    ],
  },
  {
    key: "not_interested",
    phrases: [
      // English
      "not interested", "no interest", "pass", "no thanks",
      "no thank you", "not selling", "not for sale",
      "won't sell", "will not sell", "not going to sell",
      "don't want to sell", "dont want to sell",
      "not looking to sell", "not considering selling",
      "not considering it", "keeping it", "going to keep it",
      "going to keep the property", "holding onto it",
      "not ready to part with it", "plan to keep",
      "keeping the property", "not planning to sell",
      "please don't contact me again", "no need to follow up",
      "not at this time", "don't bother", "don't bother me",
      "don't waste my time", "waste of time",
      "no need to reach out again",
      // Spanish
      "no estoy interesado", "no estoy interesada",
      "no me interesa", "no quiero vender",
      "no está en venta", "no lo voy a vender",
      "no la voy a vender", "voy a conservarlo",
      "voy a conservarla", "no gracias",
      "no se vende", "no pienso vender",
      "no tiene precio", "no me llames más",
      "no nos interesa",
      // Portuguese
      "não tenho interesse", "nao tenho interesse",
      "não quero vender", "nao quero vender",
      "não está à venda", "nao esta a venda",
      "não vou vender", "nao vou vender",
      "vou manter", "não obrigado",
      "não é para vender", "não pense em vender",
      // Italian
      "non sono interessato", "non sono interessata",
      "non voglio vendere", "non è in vendita",
      "non la venderò", "la tengo",
      "non ci penso a vendere", "nessun interesse",
      "no grazie",
      // French
      "pas intéressé", "pas interesse", "pas intéressée",
      "je ne veux pas vendre", "pas à vendre",
      "je vais garder", "aucun intérêt", "non merci",
      "je ne compte pas vendre",
      // German
      "nicht interessiert", "ich möchte nicht verkaufen",
      "ich mochte nicht verkaufen", "nicht zu verkaufen",
      "ich behalte es", "kein interesse", "nein danke",
      "kommt nicht in frage",
      // Vietnamese
      "không quan tâm", "không bán",
      "không muốn bán", "giữ lại", "không cần",
      // Polish
      "nie jestem zainteresowany", "nie jestem zainteresowana",
      "nie chcę sprzedawać", "nie na sprzedaż",
      "zatrzymam to", "nie dziękuję", "brak zainteresowania",
    ],
  },
  {
    key: "already_listed",
    phrases: [
      // English
      "listed", "listing it", "listing the property",
      "put it on the market", "on the market",
      "working with a realtor", "working with an agent",
      "working with a broker", "have an agent", "have a realtor",
      "listed with an agent", "listed with a realtor",
      "mls", "zillow", "redfin", "trulia", "realtor.com",
      "already on the market", "under contract", "in escrow",
      "pending sale", "sale pending", "have a buyer",
      "already have a buyer", "currently listed",
      "accepting offers", "offer accepted",
      // Spanish
      "ya está listada", "ya está en el mercado",
      "tengo agente", "trabajo con un agente",
      "ya tiene comprador", "está en el mercado",
      "con un agente", "ya acepté una oferta",
      "en trámite", "bajo contrato",
      // Portuguese
      "já está listado", "já está no mercado",
      "tenho um agente", "trabalho com um agente",
      "já tem comprador", "está no mercado",
      "com uma imobiliária", "já aceitei uma oferta",
      "em processo", "sob contrato",
      // Italian
      "già in vendita", "già sul mercato",
      "ho un agente", "lavoro con un agente",
      "ho già un acquirente", "è sul mercato",
      "con un agente immobiliare", "offerta accettata",
      "in trattativa",
      // French
      "déjà mis en vente", "déjà sur le marché",
      "j'ai un agent", "je travaille avec un agent",
      "j'ai déjà un acheteur", "sur le marché",
      "offre acceptée", "sous contrat",
      // German
      "schon gelistet", "schon auf dem markt",
      "habe einen makler", "arbeite mit einem makler",
      "habe schon einen käufer", "auf dem markt",
      "angebot angenommen", "unter vertrag",
      // Vietnamese
      "đã đăng bán", "đang trên thị trường",
      "có môi giới rồi", "đã có người mua",
      "đã chấp nhận đề nghị",
      // Polish
      "już wystawiony", "już na rynku",
      "mam agenta", "pracuję z agentem",
      "mam już kupca", "na rynku",
      "oferta przyjęta", "pod umową",
    ],
  },
  {
    key: "need_more_money",
    phrases: [
      // English
      "too low", "way too low", "that's low", "thats low", "too cheap",
      "need more", "need more money", "want more", "need higher",
      "higher offer", "can you do better", "best price", "best offer",
      "lowball", "low ball", "lowballing me", "not enough",
      "not even close", "nowhere near", "way off", "way under",
      "come up on the price", "raise your offer",
      "worth more than that", "more than that",
      "that won't work for me", "that doesn't work",
      "i need more than that", "expecting more",
      "was hoping for more", "is that the best you can do",
      "i expected more", "that's insulting", "thats insulting",
      "need at least", "minimum is", "my floor is",
      // Spanish
      "es muy bajo", "muy bajo", "necesito más", "necesito mas",
      "suba la oferta", "no es suficiente",
      "esperaba más", "esperaba mas", "no alcanza",
      "es una oferta ridícula", "necesito al menos",
      "mi mínimo es",
      // Portuguese
      "muito baixo", "preciso de mais", "aumente a oferta",
      "não é suficiente", "nao e suficiente",
      "esperava mais", "não chega", "é uma oferta ridícula",
      "preciso de pelo menos",
      // Italian
      "troppo basso", "ho bisogno di più",
      "aumenta l'offerta", "non è abbastanza",
      "mi aspettavo di più", "è un'offerta ridicola",
      "ho bisogno di almeno",
      // French
      "trop bas", "j'ai besoin de plus",
      "augmentez votre offre", "ce n'est pas assez",
      "je m'attendais à plus", "c'est ridicule comme offre",
      // German
      "zu niedrig", "ich brauche mehr",
      "erhöhen sie ihr angebot", "nicht genug",
      "ich habe mehr erwartet", "lächerliches angebot",
      // Vietnamese
      "thấp quá", "cần nhiều hơn",
      "tăng đề nghị lên", "không đủ",
      "mong nhiều hơn",
      // Polish
      "za mało", "potrzebuję więcej",
      "zwiększ ofertę", "to nie wystarczy",
      "spodziewałem się więcej",
    ],
  },
  {
    key: "need_time",
    phrases: [
      // English
      "need time", "need more time", "not ready yet", "not ready",
      "later", "maybe later", "sometime later", "not right now",
      "not yet", "check back", "check back later",
      "circle back", "circle back later", "follow up later",
      "not in a rush", "no rush on my end",
      "thinking about it", "still deciding", "still thinking",
      "need to think", "need to think about it",
      "not sure yet", "give me some time", "give me time",
      "next year", "few months", "down the road", "eventually",
      "not today", "not this week", "not this month",
      "maybe in a few", "will reach out when ready",
      "touch base later", "not the right time",
      "timing isn't right", "bad timing",
      // Spanish
      "necesito tiempo", "no estoy listo", "no estoy lista",
      "más adelante", "mas adelante",
      "lo estoy pensando", "todavía no", "todavia no",
      "no es el momento", "más tarde",
      "vuelve a llamar después", "cuando esté listo",
      // Portuguese
      "preciso de tempo", "não estou pronto", "nao estou pronto",
      "não estou pronta", "mais para frente",
      "ainda estou pensando", "ainda não", "ainda nao",
      "não é o momento certo", "mais tarde",
      "quando estiver pronto",
      // Italian
      "ho bisogno di tempo", "non sono pronto",
      "non sono pronta", "più avanti",
      "ci sto ancora pensando", "non ancora",
      "non è il momento giusto", "più tardi",
      "quando sarò pronto",
      // French
      "j'ai besoin de temps", "pas encore prêt", "pas encore pret",
      "plus tard", "j'y réfléchis encore",
      "pas encore", "ce n'est pas le bon moment",
      "quand je serai prêt",
      // German
      "brauche zeit", "noch nicht bereit",
      "später", "ich denke noch darüber nach",
      "noch nicht", "der zeitpunkt ist nicht gut",
      "wenn ich bereit bin",
      // Vietnamese
      "cần thêm thời gian", "chưa sẵn sàng",
      "sau này", "đang suy nghĩ",
      "chưa phải lúc", "khi nào sẵn sàng",
      // Polish
      "potrzebuję czasu", "nie jestem gotowy",
      "nie jestem gotowa", "później",
      "jeszcze myślę", "jeszcze nie",
      "to nie jest dobry moment",
    ],
  },
  {
    key: "need_family_ok",
    phrases: [
      // English
      "need to talk to my wife", "need to ask my wife",
      "check with my wife", "need to talk to my husband",
      "need to ask my husband", "check with my husband",
      "need to talk to my spouse", "check with my spouse",
      "need to talk to my partner", "check with my partner",
      "need to discuss with family", "need to talk to my family",
      "need family approval", "family needs to agree",
      "need to check with my", "need to ask my",
      "waiting on my partner", "waiting on my spouse",
      "have to run it by", "run it by my",
      "my kids need to decide", "my children need to decide",
      "co-owner needs to agree", "multiple owners need to agree",
      "need to talk to my brother", "need to talk to my sister",
      "need to talk to my mom", "need to talk to my dad",
      "need to talk to my parents", "need my family's blessing",
      "family has to agree", "all owners need to agree",
      // Spanish
      "hablar con mi esposa", "hablar con mi esposo",
      "hablar con mi familia", "necesito consultarlo",
      "mi esposa tiene que decidir", "mi esposo tiene que decidir",
      "todos los dueños tienen que estar de acuerdo",
      "mis hijos tienen que decidir",
      "consultar con mi hermano", "consultar con mi hermana",
      "necesito la aprobación de mi familia",
      // Portuguese
      "falar com minha esposa", "falar com meu marido",
      "falar com a família", "preciso consultar",
      "minha esposa precisa decidir", "meu marido precisa decidir",
      "todos os proprietários precisam concordar",
      "meus filhos precisam decidir",
      // Italian
      "parlare con mia moglie", "parlare con mio marito",
      "parlare con la famiglia", "devo consultare",
      "mia moglie deve decidere", "mio marito deve decidere",
      "tutti i proprietari devono essere d'accordo",
      // French
      "parler avec ma femme", "parler avec mon mari",
      "consulter ma famille", "ma femme doit décider",
      "mon mari doit décider", "tous les propriétaires doivent être d'accord",
      // German
      "mit meiner frau besprechen", "mit meinem mann besprechen",
      "mit der familie besprechen", "meine frau muss entscheiden",
      "mein mann muss entscheiden",
      // Vietnamese
      "hỏi ý kiến vợ", "hỏi ý kiến chồng",
      "bàn với gia đình", "vợ tôi phải đồng ý",
      "chồng tôi phải đồng ý",
      // Polish
      "porozmawiać z żoną", "porozmawiać z mężem",
      "skonsultować z rodziną", "żona musi zdecydować",
      "mąż musi zdecydować",
    ],
  },
  {
    key: "send_offer_first",
    phrases: [
      // English
      "what's your offer", "what is your offer", "what's the offer",
      "send offer", "send me an offer", "send me the offer",
      "send me a number", "send me the number", "what's the number",
      "how much can you pay", "what can you pay", "what will you pay",
      "how much will you give", "make me an offer", "give me an offer",
      "what are you offering", "what's your number", "your number first",
      "give me a number", "give me a price", "tell me the price",
      "what do you pay for properties like this",
      "send first", "offer first", "show me the number",
      "what's it worth to you", "how much is it worth to you",
      "what would you offer", "ballpark figure",
      // Spanish
      "cuánto ofrecen", "cuanto ofrecen",
      "mándame una oferta", "mandame una oferta",
      "dime el precio", "cuál es tu oferta", "cual es tu oferta",
      "cuánto me das", "cuanto me das",
      "primero la oferta", "dime un número",
      "cuánto pagas por propiedades así",
      // Portuguese
      "quanto você paga", "quanto voce paga",
      "me manda uma proposta", "qual é sua oferta",
      "qual e sua oferta", "me diz o valor",
      "quanto me oferece", "a proposta primeiro",
      "me dá um número",
      // Italian
      "quanto offri", "mandami un'offerta",
      "qual è la tua offerta", "dimmi il prezzo",
      "quanto mi dai", "prima l'offerta",
      "dammi un numero",
      // French
      "combien offrez-vous", "envoyez-moi une offre",
      "quelle est votre offre", "dites-moi le prix",
      "combien me donnez-vous", "l'offre d'abord",
      // German
      "wie viel bieten sie", "schicken sie mir ein angebot",
      "was ist ihr angebot", "sagen sie mir den preis",
      "wie viel geben sie mir", "angebot zuerst",
      // Vietnamese
      "đề nghị bao nhiêu", "gửi đề nghị cho tôi",
      "đề nghị của bạn là gì", "giá bao nhiêu",
      // Polish
      "ile oferujecie", "prześlijcie ofertę",
      "jaka jest wasza oferta", "powiedzcie mi cenę",
    ],
  },
  {
    key: "tenant_issue",
    phrases: [
      // English
      "tenant", "tenants", "renter", "renters", "occupied",
      "lease", "renting it out", "has tenants", "people living there",
      "someone living there", "can't kick them out",
      "eviction", "evicting", "need to evict",
      "section 8", "housing voucher", "hud tenant",
      "problem tenant", "bad tenant", "tenant won't leave",
      "lease ends", "lease doesn't end", "month to month",
      "long-term tenant", "tenant in place", "currently rented",
      "tenant has rights", "tenant protections",
      // Spanish
      "inquilino", "inquilinos", "arrendatario", "arrendatarios",
      "está ocupada", "está rentada", "rentada", "rentado",
      "no puedo sacar al inquilino", "está alquilada",
      "tiene inquilinos", "desalojo", "sección 8",
      "el inquilino no se quiere ir",
      // Portuguese
      "está alugado", "está alugada",
      "locatário", "locatária", "locatários",
      "não posso despejar", "tem inquilinos",
      "despejo", "alugado",
      // Italian
      "affittato", "affittata", "locatario", "locataria",
      "locatari", "non riesco a sfrattare",
      "ha degli inquilini", "sfratto",
      // French
      "loué", "louée", "locataire", "locataires",
      "ne peut pas expulser", "a des locataires",
      "expulsion",
      // German
      "vermietet", "mieter", "mieterin",
      "kann nicht rauswerfen", "hat mieter",
      "räumung",
      // Vietnamese
      "đang cho thuê", "có người thuê",
      "người thuê", "không thể đuổi",
      // Polish
      "wynajmowane", "lokator", "lokatorka",
      "nie mogę wyeksmitować", "ma lokatorów",
    ],
  },
  {
    key: "condition_bad",
    phrases: [
      // English
      "needs work", "needs a lot of work", "bad shape", "bad condition",
      "poor condition", "rough shape", "rough condition",
      "foundation issues", "foundation problems", "cracked foundation",
      "roof damage", "roof needs replacing", "bad roof",
      "fire damage", "fire damaged", "burned", "burnt",
      "flood damage", "water damage", "mold", "mold issues",
      "major repairs needed", "major issues",
      "fixer", "fixer upper", "as is", "as-is",
      "code violations", "code violation", "condemned",
      "uninhabitable", "falling apart",
      "structural issues", "structural damage", "structural problems",
      "needs everything", "gut job", "tear down", "teardown",
      "hoarder", "hoarder house", "hoarder situation",
      "needs total renovation", "major renovation needed",
      "hazardous", "environmental issues", "asbestos",
      // Spanish
      "necesita reparaciones", "está en mal estado",
      "muchos daños", "necesita mucho trabajo",
      "problemas estructurales", "daños por agua",
      "moho", "necesita todo", "está deteriorada",
      // Portuguese
      "precisa de reparos", "está em mau estado",
      "muitos danos", "precisa de muito trabalho",
      "problemas estruturais", "danos de água",
      "mofo", "precisa de tudo",
      // Italian
      "ha bisogno di riparazioni", "in cattive condizioni",
      "molti danni", "ha bisogno di molto lavoro",
      "problemi strutturali", "danni d'acqua",
      "muffa", "ha bisogno di tutto",
      // French
      "besoin de réparations", "mauvais état",
      "beaucoup de dégâts", "besoin de beaucoup de travail",
      "problèmes structurels", "dégâts des eaux",
      // German
      "renovierungsbedürftig", "schlechter zustand",
      "viele schäden", "viel arbeit nötig",
      "strukturelle probleme", "wasserschäden",
      // Vietnamese
      "cần sửa chữa", "tình trạng xấu",
      "nhiều hư hỏng", "cần nhiều việc",
      // Polish
      "wymaga remontu", "zły stan",
      "dużo uszkodzeń", "potrzeba dużo pracy",
    ],
  },
  {
    key: "probate",
    phrases: [
      // English
      "probate", "in probate", "going through probate",
      "estate", "family estate", "inherited", "inheritance",
      "inherited the property", "just inherited", "recently inherited",
      "deceased owner", "owner passed away", "owner died",
      "passed away", "recently passed", "just passed",
      "executor of the estate", "administrator of the estate",
      "estate attorney", "estate lawyer", "trust",
      "living trust", "revocable trust", "irrevocable trust",
      "heir", "heirs", "beneficiary",
      // Spanish
      "herencia", "sucesión", "sucesion",
      "heredé la propiedad", "herede la propiedad",
      "el dueño falleció", "el propietario falleció",
      "en sucesión", "albacea",
      // Portuguese
      "herança", "espólio", "espolio",
      "herdei o imóvel", "herdei o imovel",
      "o proprietário faleceu", "em inventário",
      "inventario", "executor",
      // Italian
      "eredità", "successione",
      "ho ereditato la proprietà", "il proprietario è morto",
      "in successione", "esecutore testamentario",
      // French
      "héritage", "succession",
      "j'ai hérité la propriété", "le propriétaire est décédé",
      "en succession", "exécuteur testamentaire",
      // German
      "erbschaft", "nachlassgericht",
      "ich habe das grundstück geerbt", "der eigentümer ist gestorben",
      "im nachlassverfahren", "testamentsvollstrecker",
      // Vietnamese
      "thừa kế", "di sản",
      "thừa hưởng bất động sản", "chủ nhà đã mất",
      // Polish
      "spadek", "postępowanie spadkowe",
      "odziedziczyłem nieruchomość", "właściciel zmarł",
    ],
  },
  {
    key: "divorce",
    phrases: [
      // English
      "going through divorce", "in the middle of a divorce",
      "divorce", "divorcing my",
      "my ex-wife", "my ex-husband", "my ex-spouse", "my ex",
      "separation", "separating from", "divorce settlement",
      "divorce proceedings", "court order to sell",
      "judge has to approve", "divorce decree",
      "ex won't agree", "ex has to sign",
      "marital dispute", "marriage dissolution",
      // Spanish
      "divorciándome", "divorciandome", "divorcio",
      "mi ex", "mi ex esposa", "mi ex esposo",
      "separación", "separacion",
      "orden judicial para vender", "decreto de divorcio",
      // Portuguese
      "divorciando", "divórcio", "divorcio",
      "meu ex", "minha ex-esposa", "meu ex-marido",
      "separação", "separacao",
      "ordem judicial para vender",
      // Italian
      "divorziando", "divorzio",
      "mia ex", "mia ex moglie", "mio ex marito",
      "separazione", "decreto di divorzio",
      // French
      "en train de divorcer", "divorce",
      "mon ex", "ma femme et moi divorçons",
      "séparation", "décision de justice de vendre",
      // German
      "scheidung", "in scheidung",
      "mein ex", "meine ex-frau", "mein ex-mann",
      "trennung", "gerichtliche anordnung zu verkaufen",
      // Vietnamese
      "đang ly hôn", "ly hôn",
      "vợ cũ", "chồng cũ",
      "phân ly", "tòa án yêu cầu bán",
      // Polish
      "w trakcie rozwodu", "rozwód",
      "moja była żona", "mój były mąż",
      "separacja", "nakaz sądowy sprzedaży",
    ],
  },
  {
    key: "financial_distress",
    phrases: [
      // English
      "behind on payments", "behind on mortgage", "missed payments",
      "can't make payments", "can't afford the mortgage",
      "foreclosure", "pre-foreclosure", "facing foreclosure",
      "notice of default", "default notice", "nod",
      "owe back taxes", "back taxes", "tax lien",
      "liens on the property", "judgement lien",
      "can't afford", "underwater on the mortgage",
      "owe more than it's worth", "upside down on it",
      "bankruptcy", "filing bankruptcy", "in bankruptcy",
      "chapter 7", "chapter 13", "chapter 11",
      "sheriff sale", "tax sale", "auction",
      "losing the property", "about to lose it",
      // Spanish
      "atrasado en pagos", "atrasada en pagos",
      "ejecución hipotecaria", "embargo",
      "deudas de impuestos", "no puedo pagar",
      "debo más de lo que vale",
      "declaración de quiebra", "en quiebra",
      "lien de impuestos", "por embargar",
      // Portuguese
      "atrasado nos pagamentos", "atrasada nos pagamentos",
      "execução hipotecária",
      "impostos atrasados", "não consigo pagar",
      "devo mais do que vale",
      "falência", "em falência",
      // Italian
      "in ritardo sui pagamenti",
      "preclusione", "pignoramento",
      "tasse arretrate", "non riesco a pagare",
      "devo più di quanto vale",
      "fallimento", "in bancarotta",
      // French
      "en retard sur les paiements",
      "saisie immobilière",
      "impôts impayés", "je n'arrive pas à payer",
      "je dois plus que ça vaut",
      "faillite",
      // German
      "mit zahlungen im rückstand",
      "zwangsvollstreckung",
      "rückständige steuern", "ich kann nicht zahlen",
      "ich schulde mehr als es wert ist",
      "insolvenz", "konkurs",
      // Vietnamese
      "trễ thanh toán", "tịch biên",
      "thuế chưa trả", "không thể trả",
      "nợ nhiều hơn giá trị",
      // Polish
      "zalegam z płatnościami",
      "egzekucja hipoteczna",
      "zaległe podatki", "nie mogę zapłacić",
      "jestem pod wodą",
    ],
  },
  {
    key: "has_other_buyer",
    phrases: [
      // English
      "have another offer", "got another offer", "received another offer",
      "someone else offered", "already have a buyer",
      "working with someone else", "another investor interested",
      "another buyer", "other interested parties",
      "competing offer", "better offer on the table",
      "considering other offers", "multiple offers",
      "another company offered", "someone offered more",
      // Spanish
      "tengo otra oferta", "ya tengo comprador",
      "otro comprador interesado",
      "alguien más ofreció", "otras ofertas",
      "ya me ofrecieron más",
      // Portuguese
      "tenho outra proposta", "já tenho comprador",
      "outro comprador interessado",
      "alguém mais ofereceu", "outras propostas",
      // Italian
      "ho un'altra offerta", "ho già un acquirente",
      "un altro acquirente interessato",
      "qualcun altro ha offerto",
      // French
      "j'ai une autre offre", "j'ai déjà un acheteur",
      "un autre acheteur intéressé",
      // German
      "ich habe ein anderes angebot", "ich habe schon einen käufer",
      "ein anderer interessent",
      // Vietnamese
      "có đề nghị khác rồi", "đã có người mua rồi",
      // Polish
      "mam inną ofertę", "mam już kupca",
      "inny kupiec jest zainteresowany",
    ],
  },
  {
    key: "wants_retail",
    phrases: [
      // English
      "want full price", "want full market value",
      "looking for market value", "looking for market price",
      "need retail", "not doing below market",
      "my asking price", "i know what it's worth",
      "i know the value", "worth more on the open market",
      "get more on the market", "list it for more",
      "won't take below asking", "retail price",
      "i'll get more with an agent",
      // Spanish
      "quiero precio de mercado", "quiero precio completo",
      "sé lo que vale", "se lo que vale",
      "no haré menos del mercado",
      "precio justo de mercado",
      // Portuguese
      "quero preço de mercado", "quero o valor cheio",
      "sei o que vale", "não vou aceitar menos",
      // Italian
      "voglio il prezzo di mercato", "voglio il pieno valore",
      "so quanto vale", "non accetto meno",
      // French
      "je veux le prix du marché", "je veux la valeur complète",
      "je sais ce que ça vaut", "je n'accepte pas moins",
      // German
      "ich will marktpreis", "ich will den vollen wert",
      "ich weiß was es wert ist", "nicht unter marktpreis",
      // Vietnamese
      "muốn giá thị trường", "biết giá trị",
      "không bán dưới giá thị trường",
      // Polish
      "chcę ceny rynkowej", "wiem ile to warte",
      "nie poniżej ceny rynkowej",
    ],
  },
  {
    key: "needs_call",
    phrases: [
      // English
      "call me", "give me a call", "can you call me",
      "prefer to talk", "rather talk on the phone",
      "let's talk", "phone call", "call me back",
      "call me at", "you can reach me at",
      "give me a ring", "prefer a call",
      "better to talk", "easier to explain over the phone",
      // Spanish
      "llámame", "puedes llamarme", "me llamas",
      "prefiero hablar por teléfono",
      "mejor hablamos", "llama a este número",
      // Portuguese
      "me liga", "pode me ligar", "me ligue",
      "prefiro falar por telefone",
      "melhor falar", "ligue para este número",
      // Italian
      "chiamami", "puoi chiamarmi", "mi chiama",
      "preferisco parlare per telefono",
      "meglio parlare",
      // French
      "appelez-moi", "pouvez-vous m'appeler",
      "préfère parler", "parler par téléphone",
      "appelez ce numéro",
      // German
      "ruf mich an", "können sie mich anrufen",
      "lieber telefonieren", "mich anrufen",
      // Vietnamese
      "gọi cho tôi", "bạn có thể gọi tôi",
      "muốn nói chuyện điện thoại",
      // Polish
      "zadzwoń do mnie", "czy możesz zadzwonić",
      "wolę rozmawiać przez telefon",
    ],
  },
  {
    key: "needs_email",
    phrases: [
      // English
      "email me", "send me an email", "send to my email",
      "prefer email", "contact me by email", "reach me by email",
      "send it to my email", "use my email",
      "shoot me an email",
      // Spanish
      "mándame un correo", "envíame un email",
      "enviame un email", "prefiero correo",
      "contáctame por correo",
      // Portuguese
      "me manda um e-mail", "manda um e-mail",
      "prefiro e-mail", "contate por e-mail",
      // Italian
      "mandami un'email", "preferisco l'email",
      "contattami via email",
      // French
      "envoyez-moi un email", "préfère l'email",
      "contactez-moi par email",
      // German
      "schicken sie mir eine email", "lieber per email",
      "kontaktieren sie mich per email",
      // Vietnamese
      "gửi email cho tôi", "thích liên lạc qua email",
      // Polish
      "wyślij mi email", "wolę kontakt przez email",
    ],
  },
  {
    key: "wants_written_offer",
    phrases: [
      // English
      "send it in writing", "put it in writing", "written offer",
      "send me something in writing", "want to see it in writing",
      "write it up", "send a written proposal",
      "send me the details in writing",
      "formal offer in writing",
      // Spanish
      "mándame algo por escrito", "quiero verlo por escrito",
      "envíame una propuesta escrita",
      "oferta formal por escrito",
      // Portuguese
      "me manda por escrito", "quero ver por escrito",
      "manda uma proposta escrita",
      // Italian
      "mandamelo per iscritto", "voglio vederlo per scritto",
      "proposta scritta",
      // French
      "envoyez-moi quelque chose par écrit",
      "je veux voir ça par écrit",
      "proposition écrite formelle",
      // German
      "schicken sie es schriftlich", "ich möchte es schriftlich sehen",
      "schriftliches angebot",
      // Vietnamese
      "gửi bằng văn bản", "muốn xem bằng văn bản",
      // Polish
      "prześlij na piśmie", "chcę to zobaczyć na piśmie",
    ],
  },
  {
    key: "wants_proof_of_funds",
    phrases: [
      // English
      "proof of funds", "show me proof of funds", "pof",
      "can you prove you have the money",
      "are you a real buyer", "do you actually have the money",
      "show me you can close", "verify funds",
      "are you serious", "how do i know you can close",
      // Spanish
      "prueba de fondos", "demuestra que tienes el dinero",
      "eres comprador real", "cómo sé que puedes cerrar",
      // Portuguese
      "comprovante de fundos", "prova de fundos",
      "comprova que tem o dinheiro", "como sei que pode fechar",
      // Italian
      "prova dei fondi", "dimostrazione dei fondi",
      "dimostra che hai i soldi", "come so che puoi chiudere",
      // French
      "preuve de fonds", "démonstration de fonds",
      "prouvez que vous avez l'argent",
      // German
      "nachweis der finanzmittel", "beweise dass du das geld hast",
      "wie weiß ich dass du abschließen kannst",
      // Vietnamese
      "chứng minh tài chính", "bạn có đủ tiền không",
      // Polish
      "dowód posiadania środków", "udowodnij że masz pieniądze",
    ],
  },
];

function detectObjection(message) {
  const text = lower(message);
  for (const obj of OBJECTION_MAP) {
    if (includesAny(text, obj.phrases)) return obj.key;
  }
  return null;
}

// ══════════════════════════════════════════════════════════════════════════
// EMOTION DETECTION
// ══════════════════════════════════════════════════════════════════════════

const EMOTION_MAP = [
  {
    key: "motivated",
    phrases: [
      // English
      "need to sell", "need to sell fast", "ready to sell",
      "want to sell now", "want to sell quickly",
      "want to move fast", "as soon as possible", "asap",
      "quick close", "fast close", "need a quick close",
      "i'm ready", "im ready", "ready to move forward",
      "need this gone", "want it sold", "need to get out",
      "want to get out from under it", "need to liquidate",
      "want to get rid of it", "just want it gone",
      "done with it", "ready to move on",
      "let's do it", "lets do it", "let's move forward",
      "let's get this done", "i'm in", "im in",
      "how quickly can you close", "how fast can you close",
      "need to close quickly", "need to close fast",
      "willing to sell as is", "sell it as is",
      "motivated to sell", "very motivated",
      "need the cash", "need money fast",
      "need to close soon", "close in days",
      // Spanish
      "necesito vender", "necesito vender ya",
      "listo para vender", "lista para vender",
      "quiero vender ya", "necesito vender rápido",
      "quiero cerrar rápido", "listo para firmar",
      "necesito el dinero", "qué rápido pueden cerrar",
      // Portuguese
      "preciso vender", "preciso vender já",
      "pronto para vender", "pronta para vender",
      "quero vender já", "preciso vender rápido",
      "quero fechar rápido", "pronto para assinar",
      "preciso do dinheiro",
      // Italian
      "devo vendere", "devo vendere subito",
      "sono pronto a vendere", "sono pronta a vendere",
      "voglio vendere adesso", "ho bisogno di vendere presto",
      "voglio chiudere in fretta",
      // French
      "je dois vendre", "prêt à vendre", "prête à vendre",
      "je veux vendre maintenant", "besoin de vendre vite",
      "je veux conclure rapidement",
      // German
      "muss verkaufen", "bereit zu verkaufen",
      "will jetzt verkaufen", "muss schnell verkaufen",
      "will schnell abschließen",
      // Vietnamese
      "cần bán", "sẵn sàng bán", "muốn bán ngay",
      "cần bán nhanh", "muốn đóng giao dịch nhanh",
      // Polish
      "muszę sprzedać", "gotowy do sprzedaży",
      "chcę sprzedać teraz", "muszę sprzedać szybko",
    ],
  },
  {
    key: "curious",
    phrases: [
      // English
      "how does it work", "tell me more", "what's the process",
      "what are the steps", "can you explain", "how soon can you close",
      "what happens next", "more information", "more info",
      "how long does it take", "walk me through",
      "explain the process", "how does this work",
      "what do i need to do", "what's involved", "what's next",
      "is there a contract", "what kind of contract",
      "are there any fees", "any fees", "any costs",
      "do i need a lawyer", "do i need a realtor",
      "is this all cash", "how do you pay",
      "how do closings work", "what is escrow",
      "how much do you typically pay",
      "what makes you different", "why should i use you",
      "how are you different from a realtor",
      "tell me about the process", "curious about this",
      // Spanish
      "cómo funciona", "como funciona",
      "explícame más", "explicame mas",
      "qué sigue", "cuánto tiempo tarda",
      "qué necesito hacer", "hay costos",
      "necesito abogado", "qué tipo de contrato",
      "cómo pagan", "qué es un cierre",
      "cuánto suelen pagar",
      // Portuguese
      "como funciona", "me explica mais",
      "o que acontece depois", "quanto tempo leva",
      "o que preciso fazer", "tem custos",
      "preciso de advogado", "que tipo de contrato",
      "como pagam", "o que é escrow",
      // Italian
      "come funziona", "spiegami di più",
      "cosa succede dopo", "quanto tempo ci vuole",
      "cosa devo fare", "ci sono costi",
      "ho bisogno di un avvocato", "che tipo di contratto",
      "come pagate",
      // French
      "comment ça marche", "dites-m'en plus",
      "que se passe-t-il ensuite", "combien de temps",
      "qu'est-ce que je dois faire", "y a-t-il des frais",
      "comment payez-vous",
      // German
      "wie funktioniert das", "erzähl mir mehr",
      "was passiert als nächstes", "wie lange dauert es",
      "was muss ich tun", "gibt es kosten",
      "wie zahlen sie",
      // Vietnamese
      "cách hoạt động như thế nào", "cho tôi biết thêm",
      "điều gì xảy ra tiếp theo", "mất bao lâu",
      "tôi cần làm gì",
      // Polish
      "jak to działa", "powiedz mi więcej",
      "co się dzieje dalej", "jak długo to trwa",
      "co muszę zrobić",
    ],
  },
  {
    key: "skeptical",
    phrases: [
      // English
      "sounds fake", "seems fake", "feels fake",
      "are you legit", "is this legit",
      "this sounds like a scam", "sounds like a scam",
      "is this a scam", "scam", "sounds shady",
      "prove it", "prove yourself", "prove you're real",
      "not sure about this", "not convinced",
      "why should i trust you", "why should i believe you",
      "can you prove", "show me proof", "sounds too good",
      "too good to be true", "what's the catch",
      "what are you really", "something feels off",
      "i've heard this before", "i've been burned before",
      "done this before and got burned",
      "researching you", "looking you up",
      "taking my time", "doing my research",
      "not falling for this", "seen this before",
      // Spanish
      "parece fraude", "suena a estafa",
      "es una estafa", "qué trampa tiene",
      "no me convence", "parece sospechoso",
      "demuestren que son reales",
      "he sido engañado antes",
      // Portuguese
      "parece golpe", "parece fraude",
      "é golpe", "qual é a pegadinha",
      "não me convence", "parece suspeito",
      "provem que são reais",
      // Italian
      "sembra una truffa", "troppo bello per essere vero",
      "qual è la fregatura", "non mi convince",
      "sembra strano",
      // French
      "ça ressemble à une arnaque", "trop beau pour être vrai",
      "quel est le piège", "je ne suis pas convaincu",
      "ça semble louche",
      // German
      "klingt nach betrug", "zu gut um wahr zu sein",
      "was ist der haken", "ich bin nicht überzeugt",
      "klingt dubios",
      // Vietnamese
      "nghe như lừa đảo", "quá tốt để tin",
      "có gì đó không ổn", "không tin",
      // Polish
      "brzmi jak oszustwo", "zbyt piękne żeby prawdziwe",
      "co jest haczykiem", "nie jestem przekonany",
    ],
  },
  {
    key: "frustrated",
    phrases: [
      // English
      "leave me alone", "stop bothering me", "stop harassing me",
      "you're annoying", "this is annoying", "very annoying",
      "tired of this", "sick of these texts", "sick of this",
      "wasting my time", "stop wasting my time",
      "quit texting me", "quit calling me",
      "how many times do i have to say",
      "i've told you", "told you already", "already told you",
      "keep texting me", "keep calling me", "won't stop",
      "harassment", "harassing me", "this is harassment",
      "get off my back", "back off",
      // Spanish
      "déjame en paz", "dejame en paz",
      "deja de molestarme", "me tienes harto",
      "me tienes harta", "ya te dije", "qué pesado",
      "qué pesada", "me estás acosando",
      "dejen de llamarme",
      // Portuguese
      "me deixa em paz", "para de me incomodar",
      "você está me incomodando", "já te disse",
      "que chato", "que chata", "me estás assediando",
      // Italian
      "lasciami in pace", "smettila di disturbarmi",
      "mi stai dando fastidio", "te l'ho già detto",
      "che seccatura", "mi stai molestando",
      // French
      "laissez-moi tranquille", "arrêtez de me déranger",
      "vous m'ennuyez", "je vous l'ai déjà dit",
      "quelle nuisance", "vous me harcelez",
      // German
      "lass mich in ruhe", "hör auf mich zu belästigen",
      "du nervst mich", "ich habe es dir bereits gesagt",
      "du belästigst mich",
      // Vietnamese
      "để tôi yên", "thôi làm phiền tôi",
      "bạn làm tôi khó chịu", "đã nói rồi",
      // Polish
      "zostaw mnie w spokoju", "przestań mi przeszkadzać",
      "irytujące", "już ci mówiłem",
    ],
  },
  {
    key: "tired_landlord",
    phrases: [
      // English
      "tenant headache", "tenant problems", "problem tenants",
      "done with tenants", "tired of tenants", "tired landlord",
      "over being a landlord", "sick of being a landlord",
      "sick of dealing with it", "just want out",
      "tired of the property", "headache property",
      "management nightmare", "property management headache",
      "eviction nightmare", "eviction process is exhausting",
      "can't take it anymore", "too much trouble",
      "not worth the hassle", "not worth it anymore",
      "dealing with tenants is exhausting",
      "rental is a nightmare", "no longer want to be a landlord",
      "done with rentals", "don't want to manage it anymore",
      // Spanish
      "cansado de los inquilinos", "cansada de los inquilinos",
      "ya no quiero ser arrendador",
      "ya no quiero ser arrendadora",
      "los inquilinos me tienen harto",
      "los inquilinos me tienen harta",
      "harto de ser casero", "harta de ser casera",
      // Spanish (continued)
      "ya no quiero rentas", "no quiero seguir rentando",
      "harto de administrar", "cansado de administrar",
      // Portuguese
      "cansado dos inquilinos", "cansada dos inquilinos",
      "não quero mais ser proprietário",
      "não quero mais ser proprietária",
      "os inquilinos estão me deixando louco",
      "os inquilinos estão me deixando louca",
      "farto de ser senhorio", "farta de ser senhoria",
      "não quero mais alugar", "cansado de administrar",
      // Italian
      "stanco degli inquilini", "stanca degli inquilini",
      "non voglio più fare il padrone di casa",
      "gli inquilini mi fanno impazzire",
      "stufo di essere proprietario",
      "non voglio più gestirlo",
      // French
      "fatigué des locataires", "fatiguée des locataires",
      "je ne veux plus être propriétaire",
      "les locataires me rendent fou",
      "marre d'être bailleur", "en avoir assez de gérer ça",
      // German
      "müde von den mietern", "ich will kein vermieter mehr sein",
      "die mieter machen mich wahnsinnig",
      "genug von der vermietung",
      "will es nicht mehr verwalten",
      // Vietnamese
      "chán người thuê", "không muốn làm chủ nhà nữa",
      "người thuê làm tôi điên", "mệt mỏi với việc cho thuê",
      // Polish
      "zmęczony lokatorami", "zmęczona lokatorami",
      "nie chcę już być właścicielem",
      "lokatorzy mnie wykańczają",
      "dość wynajmowania",
    ],
  },
  {
    key: "overwhelmed",
    phrases: [
      // English
      "a lot going on", "overwhelmed", "too much going on",
      "dealing with a lot", "in over my head",
      "very stressful", "extremely stressful", "so stressful",
      "too much right now", "too much on my plate",
      "going through divorce", "divorce is messy",
      "family issues", "family problems", "family conflict",
      "health issues", "dealing with illness", "health scare",
      "probate is complicated", "estate is a mess",
      "don't know where to start", "don't know what to do",
      "everything is happening at once",
      "life is complicated right now", "really complicated situation",
      "hard to think straight", "hard to focus",
      // Spanish
      "mucho pasando", "muy estresante",
      "no sé qué hacer", "no se que hacer",
      "muchas cosas al mismo tiempo",
      "situación complicada", "muy complicado ahora",
      "no sé por dónde empezar",
      "la vida está complicada",
      // Portuguese
      "muita coisa acontecendo", "muito estressante",
      "não sei o que fazer", "nao sei o que fazer",
      "muitas coisas ao mesmo tempo",
      "situação complicada", "muito complicado agora",
      "não sei por onde começar",
      // Italian
      "tanto da fare", "molto stressante",
      "non so cosa fare", "tante cose allo stesso tempo",
      "situazione complicata", "molto complicato adesso",
      "non so da dove cominciare",
      // French
      "beaucoup à gérer", "très stressant",
      "je ne sais pas quoi faire",
      "trop de choses en même temps",
      "situation compliquée", "je ne sais pas par où commencer",
      // German
      "viel los gerade", "sehr stressig",
      "ich weiß nicht was ich tun soll",
      "zu viele dinge gleichzeitig",
      "komplizierte situation", "weiß nicht wo ich anfangen soll",
      // Vietnamese
      "nhiều chuyện đang xảy ra", "rất căng thẳng",
      "không biết phải làm gì",
      "quá nhiều thứ cùng lúc",
      "tình huống phức tạp",
      // Polish
      "dużo się dzieje", "bardzo stresujące",
      "nie wiem co zrobić",
      "za dużo rzeczy naraz",
      "skomplikowana sytuacja",
    ],
  },
  {
    key: "grieving",
    phrases: [
      // English
      "my mother passed", "my father passed", "my parent passed",
      "my husband passed", "my wife passed", "my spouse passed",
      "my sibling passed", "my brother passed", "my sister passed",
      "my grandmother passed", "my grandfather passed",
      "my uncle passed", "my aunt passed",
      "lost my mother", "lost my father", "lost my husband",
      "lost my wife", "lost my spouse", "lost my parent",
      "he passed away", "she passed away", "they passed away",
      "recently passed away", "just passed away",
      "after the funeral", "after they passed", "after she passed",
      "after he passed", "dealing with a loss", "dealing with grief",
      "estate of my", "just lost", "we lost",
      "gone too soon", "in mourning", "still grieving",
      "haven't recovered from the loss",
      // Spanish
      "mi madre falleció", "mi padre falleció",
      "mi esposa falleció", "mi esposo falleció",
      "mi hermano falleció", "mi hermana falleció",
      "mi abuela falleció", "mi abuelo falleció",
      "perdí a mi", "acaba de fallecer",
      "recién falleció", "estamos de luto",
      "todavía en duelo", "después del funeral",
      "después de que falleció",
      // Portuguese
      "minha mãe faleceu", "meu pai faleceu",
      "minha esposa faleceu", "meu marido faleceu",
      "meu irmão faleceu", "minha irmã faleceu",
      "minha avó faleceu", "meu avô faleceu",
      "perdi meu", "perdi minha",
      "acabou de falecer", "recentemente faleceu",
      "estamos de luto", "ainda de luto",
      "depois do funeral",
      // Italian
      "mia madre è morta", "mio padre è morto",
      "mia moglie è morta", "mio marito è morto",
      "mio fratello è morto", "mia sorella è morta",
      "mia nonna è morta", "mio nonno è morto",
      "ho perso", "è appena morto", "è appena morta",
      "siamo in lutto", "ancora in lutto",
      "dopo il funerale",
      // French
      "ma mère est décédée", "mon père est décédé",
      "ma femme est décédée", "mon mari est décédé",
      "mon frère est décédé", "ma sœur est décédée",
      "j'ai perdu", "vient de décéder",
      "nous sommes en deuil", "encore en deuil",
      "après les funérailles",
      // German
      "meine mutter ist gestorben", "mein vater ist gestorben",
      "meine frau ist gestorben", "mein mann ist gestorben",
      "mein bruder ist gestorben", "meine schwester ist gestorben",
      "ich habe verloren", "gerade gestorben",
      "wir trauern", "noch in trauer",
      "nach der beerdigung",
      // Vietnamese
      "mẹ tôi mất", "bố tôi mất",
      "vợ tôi mất", "chồng tôi mất",
      "anh tôi mất", "chị tôi mất",
      "tôi mất đi", "vừa mới mất",
      "đang để tang", "sau đám tang",
      // Polish
      "moja matka zmarła", "mój ojciec zmarł",
      "moja żona zmarła", "mój mąż zmarł",
      "mój brat zmarł", "moja siostra zmarła",
      "straciłem", "straciłam",
      "właśnie zmarł", "właśnie zmarła",
      "jesteśmy w żałobie", "po pogrzebie",
    ],
  },
];

function detectEmotion(message) {
  const text = lower(message);
  for (const emotion of EMOTION_MAP) {
    if (includesAny(text, emotion.phrases)) return emotion.key;
  }
  return wordCount(text) <= 2 ? "guarded" : "calm";
}

// ══════════════════════════════════════════════════════════════════════════
// POSITIVE SIGNAL DETECTION
// ══════════════════════════════════════════════════════════════════════════

const POSITIVE_SIGNAL_MAP = [
  {
    key: "affirmative",
    phrases: [
      // English
      "yes", "yeah", "yep", "yup", "sure", "ok", "okay",
      "sounds good", "interested", "tell me more", "go ahead",
      "let's talk", "i'm open", "open to it",
      "open to hearing", "willing to listen",
      "could work", "might work", "worth discussing",
      "absolutely", "definitely", "for sure", "roger that",
      // Spanish
      "sí", "si", "claro", "por supuesto", "de acuerdo",
      "está bien", "sale", "órale", "ándale",
      "me interesa", "adelante", "cuéntame más",
      // Portuguese
      "sim", "claro", "com certeza", "tudo certo",
      "me interessa", "pode falar", "conte mais",
      "beleza", "tá bom",
      // Italian
      "sì", "certo", "certamente", "va bene",
      "mi interessa", "dimmi di più", "assolutamente",
      // French
      "oui", "bien sûr", "d'accord", "absolument",
      "ça m'intéresse", "dites-moi plus",
      // German
      "ja", "natürlich", "gut", "in ordnung",
      "interessiert mich", "erzähl mir mehr",
      // Vietnamese
      "vâng", "được", "có", "đồng ý",
      "quan tâm", "cho tôi biết thêm",
      // Polish
      "tak", "oczywiście", "dobrze", "zgadzam się",
      "interesuje mnie", "powiedz mi więcej",
    ],
  },
  {
    key: "vacant_property",
    phrases: [
      // English
      "vacant", "empty", "nobody living there", "no one lives there",
      "no one there", "unoccupied", "sitting empty", "been empty",
      "vacant for", "no tenants", "no one in it",
      "abandoned", "sitting vacant",
      // Spanish
      "vacía", "vacia", "desocupada", "nadie vive ahí",
      "nadie vive alli", "sin inquilinos", "está vacía",
      "abandonada",
      // Portuguese
      "vazia", "desocupada", "ninguém mora lá",
      "sem inquilinos", "está vazia", "abandonada",
      // Italian
      "vuota", "disabitata", "nessuno ci abita",
      "senza inquilini", "è vuota", "abbandonata",
      // French
      "vide", "inoccupée", "personne n'y habite",
      "sans locataires", "abandonnée",
      // German
      "leer", "unbesetzt", "niemand wohnt dort",
      "keine mieter", "verlassen",
      // Vietnamese
      "trống không", "không có người ở",
      "không có người thuê", "bỏ hoang",
      // Polish
      "pusta", "niezamieszkała", "nikt tam nie mieszka",
      "bez lokatorów", "opuszczona",
    ],
  },
  {
    key: "urgency",
    phrases: [
      // English
      "asap", "fast", "quickly", "soon", "this week",
      "this month", "immediately", "urgent", "right away",
      "as soon as possible", "need to move fast",
      "need to close fast", "no time to waste",
      "in a hurry", "time sensitive",
      // Spanish
      "pronto", "urgente", "lo antes posible",
      "esta semana", "este mes", "de inmediato",
      "necesito cerrar rápido", "sin tiempo que perder",
      // Portuguese
      "rápido", "urgente", "o mais rápido possível",
      "esta semana", "este mês", "imediatamente",
      "preciso fechar rápido",
      // Italian
      "presto", "urgente", "il prima possibile",
      "questa settimana", "questo mese", "immediatamente",
      "ho bisogno di chiudere velocemente",
      // French
      "vite", "urgent", "le plus tôt possible",
      "cette semaine", "ce mois", "immédiatement",
      // German
      "schnell", "dringend", "so bald wie möglich",
      "diese woche", "diesen monat", "sofort",
      // Vietnamese
      "nhanh", "gấp", "càng sớm càng tốt",
      "tuần này", "tháng này", "ngay lập tức",
      // Polish
      "szybko", "pilne", "jak najszybciej",
      "w tym tygodniu", "w tym miesiącu", "natychmiast",
    ],
  },
  {
    key: "cash_aware",
    phrases: [
      // English
      "cash", "all cash", "cash offer", "cash buyer",
      "cash sale", "pay cash", "buying with cash",
      "no financing", "no mortgage needed",
      // Spanish
      "efectivo", "pago en efectivo", "oferta en efectivo",
      "comprador de efectivo", "sin financiamiento",
      // Portuguese
      "dinheiro", "à vista", "oferta à vista",
      "comprador à vista", "sem financiamento",
      // Italian
      "contanti", "pagamento in contanti",
      "offerta in contanti", "senza finanziamento",
      // French
      "comptant", "paiement comptant",
      "offre comptant", "sans financement",
      // German
      "bar", "barzahlung", "barangebot",
      "ohne finanzierung",
      // Vietnamese
      "tiền mặt", "trả tiền mặt",
      "đề nghị tiền mặt", "không vay",
      // Polish
      "gotówka", "płatność gotówką",
      "oferta gotówkowa", "bez finansowania",
    ],
  },
  {
    key: "as_is_willing",
    phrases: [
      // English
      "as is", "as-is", "no repairs", "sell as is",
      "willing to sell as is", "don't want to fix anything",
      "don't want to do repairs", "sell it the way it is",
      "won't fix anything",
      // Spanish
      "como está", "tal como está", "sin reparaciones",
      "vender como está", "no quiero arreglar nada",
      // Portuguese
      "como está", "tal como está", "sem reparos",
      "vender como está", "não quero consertar nada",
      // Italian
      "così com'è", "senza riparazioni",
      "vendere così com'è", "non voglio aggiustare nulla",
      // French
      "tel quel", "sans réparations",
      "vendre tel quel", "je ne veux rien réparer",
      // German
      "so wie es ist", "ohne reparaturen",
      "verkaufen wie es ist", "will nichts reparieren",
      // Vietnamese
      "nguyên trạng", "không sửa chữa",
      "bán nguyên trạng",
      // Polish
      "w stanie jakim jest", "bez napraw",
      "sprzedać w stanie jakim jest",
    ],
  },
  {
    key: "financial_pressure",
    phrases: [
      // English
      "owe", "behind", "foreclosure", "liens on",
      "tax lien", "back taxes", "can't afford",
      "under water", "need the money", "financial pressure",
      "desperate", "losing the house", "about to lose",
      // Spanish
      "debo", "atrasado", "ejecución", "gravámenes",
      "impuestos atrasados", "no puedo pagar",
      "necesito el dinero", "presión financiera",
      "desesperado", "perdiendo la casa",
      // Portuguese
      "devo", "atrasado", "execução", "ônus",
      "impostos atrasados", "não posso pagar",
      "preciso do dinheiro", "pressão financeira",
      "desesperado", "perdendo a casa",
      // Italian
      "devo", "in ritardo", "preclusione", "gravami",
      "tasse arretrate", "non posso pagare",
      "ho bisogno di soldi", "pressione finanziaria",
      // French
      "je dois", "en retard", "saisie", "hypothèque",
      "impôts impayés", "je ne peux pas payer",
      "j'ai besoin d'argent", "pression financière",
      // German
      "ich schulde", "rückstand", "zwangsvollstreckung",
      "steuerschulden", "kann nicht zahlen",
      "brauche geld", "finanzieller druck",
      // Vietnamese
      "nợ", "trễ hạn", "tịch thu", "thuế nợ",
      "không trả được", "cần tiền",
      // Polish
      "jestem winien", "zalegam", "egzekucja",
      "zaległe podatki", "nie mogę zapłacić",
      "potrzebuję pieniędzy",
    ],
  },
  {
    key: "absentee_owner",
    phrases: [
      // English
      "out of state", "moved away", "don't live there",
      "live far", "relocated", "don't live at the property",
      "not local", "live in another state",
      "inherited and don't live there",
      "managing from a distance",
      // Spanish
      "fuera del estado", "me mudé", "no vivo ahí",
      "vivo lejos", "me reubiqué", "no soy local",
      "vivo en otro estado", "lo administro desde lejos",
      // Portuguese
      "fora do estado", "me mudei", "não moro lá",
      "moro longe", "me mudei para outro lugar",
      "não sou local", "moro em outro estado",
      // Italian
      "fuori stato", "mi sono trasferito",
      "non ci abito", "vivo lontano",
      "non sono del posto", "vivo in un altro stato",
      // French
      "hors état", "j'ai déménagé", "je n'y habite pas",
      "j'habite loin", "je ne suis pas local",
      // German
      "außerhalb des bundesstaates", "ich bin umgezogen",
      "ich wohne nicht dort", "ich wohne weit weg",
      "ich bin nicht von hier",
      // Vietnamese
      "ở tiểu bang khác", "đã chuyển đi",
      "không sống ở đó", "sống xa",
      // Polish
      "poza stanem", "przeprowadziłem się",
      "nie mieszkam tam", "mieszkam daleko",
    ],
  },
  {
    key: "price_curious",
    phrases: [
      // English
      "how much", "what's your offer", "what do you offer",
      "curious about the price", "what are you paying",
      "what would you pay", "ballpark", "rough estimate",
      "give me a range",
      // Spanish
      "cuánto", "cuanto", "qué ofrecen",
      "curioso sobre el precio", "cuánto pagan",
      "cuánto pagarían", "un estimado",
      // Portuguese
      "quanto", "qual é a oferta", "curioso sobre o preço",
      "quanto pagam", "quanto pagariam", "uma estimativa",
      // Italian
      "quanto", "qual è l'offerta",
      "curioso sul prezzo", "quanto pagate",
      "quanto paghereste",
      // French
      "combien", "quelle est l'offre",
      "curieux sur le prix", "combien payez-vous",
      // German
      "wie viel", "was ist ihr angebot",
      "neugierig auf den preis", "wie viel zahlen sie",
      // Vietnamese
      "bao nhiêu", "đề nghị bao nhiêu",
      "tò mò về giá", "trả bao nhiêu",
      // Polish
      "ile", "jaka jest oferta",
      "ciekaw ceny", "ile płacicie",
    ],
  },
  {
    key: "inherited",
    phrases: [
      // English
      "inherited", "just inherited", "recently inherited",
      "inherited the property", "left to me", "left it to me",
      "was left to me", "family left me",
      // Spanish
      "heredé", "recién heredé", "recientemente heredé",
      "me dejaron", "me lo dejaron",
      // Portuguese
      "herdei", "acabei de herdar", "recentemente herdei",
      "me deixaram",
      // Italian
      "ho ereditato", "ho appena ereditato",
      "mi hanno lasciato",
      // French
      "j'ai hérité", "je viens d'hériter",
      "on me l'a laissé",
      // German
      "ich habe geerbt", "ich habe gerade geerbt",
      "man hat es mir hinterlassen",
      // Vietnamese
      "thừa kế", "vừa thừa kế", "mới thừa kế",
      "được để lại",
      // Polish
      "odziedziczyłem", "właśnie odziedziczyłem",
      "mi to zostawiono",
    ],
  },
  {
    key: "multiple_properties",
    phrases: [
      // English
      "multiple properties", "several properties", "portfolio",
      "i have others", "other properties", "more than one property",
      "few properties", "a few properties", "handful of properties",
      // Spanish
      "varias propiedades", "múltiples propiedades",
      "tengo otras", "otras propiedades",
      "más de una propiedad", "cartera de propiedades",
      // Portuguese
      "várias propriedades", "múltiplas propriedades",
      "tenho outras", "outras propriedades",
      "mais de uma propriedade", "portfólio",
      // Italian
      "più proprietà", "diverse proprietà",
      "ne ho altre", "più di una proprietà", "portafoglio",
      // French
      "plusieurs propriétés", "d'autres propriétés",
      "plus d'une propriété", "portefeuille",
      // German
      "mehrere immobilien", "andere immobilien",
      "mehr als eine immobilie", "portfolio",
      // Vietnamese
      "nhiều bất động sản", "vài bất động sản",
      "có những cái khác",
      // Polish
      "kilka nieruchomości", "inne nieruchomości",
      "więcej niż jedna nieruchomość", "portfel",
    ],
  },
];

function detectPositiveSignals(message) {
  const text = lower(message);
  const signals = [];
  for (const signal of POSITIVE_SIGNAL_MAP) {
    if (includesAny(text, signal.phrases)) signals.push(signal.key);
  }
  return signals;
}

// ══════════════════════════════════════════════════════════════════════════
// STAGE HINT DETECTION
// ══════════════════════════════════════════════════════════════════════════

function detectStageHint(message, brain_item = null, objection = null) {
  // Objection-driven overrides — highest priority
  if (objection === "who_is_this" || objection === "wrong_number") return "Ownership";
  if (
    objection === "send_offer_first"  ||
    objection === "need_more_money"   ||
    objection === "wants_proof_of_funds"
  ) return "Offer";
  if (
    objection === "needs_call"        ||
    objection === "needs_email"       ||
    objection === "probate"           ||
    objection === "divorce"           ||
    objection === "wants_written_offer"
  ) return "Q/A";

  const existing_stage = normalizeStage(
    getCategoryValue(brain_item, "conversation-stage", "Ownership")
  );
  const text = lower(message);

  if (includesAny(text, [
    "contract", "agreement", "sign", "docusign", "docu sign",
    "send paperwork", "email the docs", "purchase agreement",
    "psa", "send the contract", "sign the contract",
    "send over the contract", "ready to sign",
    "title", "title company", "escrow", "closing",
    "close on", "closing date", "when can we close",
    "set a closing date", "open escrow",
    // Spanish
    "contrato", "firmar", "papeles", "título",
    "cierre", "fecha de cierre",
    // Portuguese
    "contrato", "assinar", "papéis", "título",
    "fechamento", "data de fechamento",
    // Italian
    "contratto", "firmare", "documenti", "titolo",
    "chiusura", "data di chiusura",
    // French
    "contrat", "signer", "documents", "titre",
    "clôture", "date de clôture",
    // German
    "vertrag", "unterzeichnen", "dokumente", "titel",
    "abschluss", "abschlussdatum",
  ])) return "Contract";

  if (includesAny(text, [
    "offer", "price", "number", "how much", "cash offer",
    "what will you pay", "what can you pay",
    "give me a number", "show me a number",
    // Spanish
    "oferta", "precio", "número", "cuánto",
    // Portuguese
    "oferta", "preço", "número", "quanto",
    // Italian
    "offerta", "prezzo", "numero", "quanto",
    // French
    "offre", "prix", "numéro", "combien",
    // German
    "angebot", "preis", "nummer", "wie viel",
  ])) return "Offer";

  if (includesAny(text, [
    "how does it work", "tell me more", "what's the process",
    "how long", "any fees", "explain", "questions",
    "want to understand", "walk me through",
    // Spanish
    "cómo funciona", "explícame", "preguntas", "tarifas",
    // Portuguese
    "como funciona", "me explica", "perguntas", "taxas",
    // Italian
    "come funziona", "spiegami", "domande", "costi",
    // French
    "comment ça marche", "expliquez", "questions", "frais",
    // German
    "wie funktioniert", "erklären", "fragen", "kosten",
  ])) return "Q/A";

  if (includesAny(text, [
    "follow up", "check back", "later", "not now",
    "next week", "next month", "in a few months",
    "circle back", "not ready yet", "reach back out",
    "i'm reconsidering", "changed my mind", "thought about it more",
    // Spanish
    "más adelante", "no ahora", "la próxima semana",
    "lo estoy reconsiderando", "cambié de opinión",
    // Portuguese
    "mais tarde", "não agora", "na próxima semana",
    "estou reconsiderando", "mudei de ideia",
    // Italian
    "più tardi", "non ora", "la settimana prossima",
    "sto riconsiderando", "ho cambiato idea",
    // French
    "plus tard", "pas maintenant", "la semaine prochaine",
    "je reconsidère", "j'ai changé d'avis",
    // German
    "später", "nicht jetzt", "nächste woche",
    "ich überlege es mir nochmal", "ich habe meine meinung geändert",
  ])) return "Follow-Up";

  return existing_stage || "Ownership";
}

// ══════════════════════════════════════════════════════════════════════════
// HEURISTIC CONFIDENCE SCORING
// ══════════════════════════════════════════════════════════════════════════

function computeHeuristicConfidence({
  objection,
  emotion,
  compliance_flag,
  language,
  positive_signals,
}) {
  if (compliance_flag !== null) return 0.99;

  // High-certainty objections get a fixed ceiling
  const FIXED_CONFIDENCE = {
    wrong_number:       0.97,
    not_interested:     0.92,
    already_listed:     0.92,
    financial_distress: 0.91,
    divorce:            0.91,
    probate:            0.91,
  };

  let confidence = FIXED_CONFIDENCE[objection] ?? 0.60;

  // Only apply additive scoring if we didn't fix a ceiling above
  if (!(objection in FIXED_CONFIDENCE)) {
    if (objection !== null) confidence += 0.18;

    if (emotion === "motivated")     confidence += 0.15;
    if (emotion === "frustrated")    confidence += 0.12;
    if (emotion === "tired_landlord") confidence += 0.12;
    if (emotion === "grieving")      confidence += 0.10;
    if (emotion === "curious")       confidence += 0.08;
    if (emotion === "overwhelmed")   confidence += 0.06;

    confidence += 0.04 * Math.min(positive_signals.length, 3);
  }

  // Script-detected languages are near-certain
  const SCRIPT_LANGUAGES = new Set([
    "Hebrew", "Mandarin", "Korean", "Arabic",
    "Russian", "Hindi", "Thai", "Japanese",
  ]);
  if (SCRIPT_LANGUAGES.has(language)) {
    confidence = Math.max(confidence, 0.95);
  }

  return Math.min(0.98, confidence);
}

// ══════════════════════════════════════════════════════════════════════════
// MOTIVATION SCORE
// ══════════════════════════════════════════════════════════════════════════

function estimateMotivationScore({ objection, emotion, positive_signals = [] }) {
  if (objection === "wrong_number") return 0;

  let score = 50;

  // Emotion deltas
  const EMOTION_DELTA = {
    motivated:     +30,
    curious:       +12,
    tired_landlord: +20,
    overwhelmed:   +12,
    grieving:      +8,
    frustrated:    -15,
    skeptical:     -10,
    guarded:       -5,
  };
  score += EMOTION_DELTA[emotion] ?? 0;

  // Objection deltas
  const OBJECTION_DELTA = {
    need_more_money:      -5,
    not_interested:       -30,
    already_listed:       -15,
    need_time:            -10,
    wants_retail:         -20,
    has_other_buyer:      -12,
    need_family_ok:       -5,
    financial_distress:   +25,
    tenant_issue:         +15,
    condition_bad:        +10,
    probate:              +10,
    divorce:              +15,
    send_offer_first:     +8,
    needs_call:           +5,
    wants_written_offer:  +5,
    wants_proof_of_funds: +5,
  };
  score += OBJECTION_DELTA[objection] ?? 0;

  // Positive signal deltas
  const SIGNAL_DELTA = {
    urgency:           +15,
    vacant_property:   +12,
    financial_pressure: +20,
    absentee_owner:    +10,
    as_is_willing:     +8,
    price_curious:     +6,
    affirmative:       +10,
    inherited:         +8,
    multiple_properties: +5,
    cash_aware:        +4,
  };
  for (const signal of positive_signals) {
    score += SIGNAL_DELTA[signal] ?? 0;
  }

  return Math.max(0, Math.min(100, score));
}

// ══════════════════════════════════════════════════════════════════════════
// HEURISTIC CLASSIFICATION
// ══════════════════════════════════════════════════════════════════════════

function classifyHeuristic(message, brain_item = null) {
  const compliance_flag  = detectComplianceFlag(message);
  const language         = detectLanguageHeuristic(message, brain_item);
  const objection        = compliance_flag ? null : detectObjection(message);
  const emotion          = detectEmotion(message);
  const positive_signals = detectPositiveSignals(message);
  const stage_hint       = detectStageHint(message, brain_item, objection);

  const confidence = computeHeuristicConfidence({
    objection,
    emotion,
    compliance_flag,
    language,
    positive_signals,
  });

  const motivation_score = estimateMotivationScore({
    objection,
    emotion,
    positive_signals,
  });

  return {
    language,
    objection,
    emotion,
    stage_hint,
    compliance_flag,
    positive_signals,
    confidence,
    motivation_score,
  };
}

// ══════════════════════════════════════════════════════════════════════════
// AI ASSIST CLASSIFICATION
// ══════════════════════════════════════════════════════════════════════════

const VALID_LANGUAGES   = new Set(["English","Spanish","Portuguese","Italian","Hebrew","Mandarin","Korean","Vietnamese","Polish","Arabic","Hindi","French","Russian","Japanese","Farsi","German","Greek","Thai","Pashto"]);
const VALID_OBJECTIONS  = new Set(["wrong_number","who_is_this","not_interested","already_listed","need_more_money","need_time","need_family_ok","send_offer_first","tenant_issue","condition_bad","probate","divorce","financial_distress","has_other_buyer","wants_retail","needs_call","needs_email","wants_written_offer","wants_proof_of_funds","null",null]);
const VALID_EMOTIONS    = new Set(["calm","skeptical","guarded","frustrated","curious","motivated","tired_landlord","overwhelmed","grieving"]);
const VALID_STAGES      = new Set(["Ownership","Offer","Q/A","Contract","Follow-Up"]);
const VALID_COMPLIANCE  = new Set(["stop_texting","null",null]);
const VALID_SIGNALS     = new Set(["affirmative","vacant_property","urgency","cash_aware","as_is_willing","financial_pressure","absentee_owner","price_curious","inherited","multiple_properties"]);

function sanitizeAiResult(raw, heuristic) {
  const language  = VALID_LANGUAGES.has(raw?.language)  ? raw.language  : heuristic.language;
  const objection = VALID_OBJECTIONS.has(raw?.objection) ? (raw.objection === "null" ? null : raw.objection) : heuristic.objection;
  const emotion   = VALID_EMOTIONS.has(raw?.emotion)    ? raw.emotion   : heuristic.emotion;
  const stage_hint = VALID_STAGES.has(raw?.stage_hint)  ? raw.stage_hint : heuristic.stage_hint;
  const compliance_flag = VALID_COMPLIANCE.has(raw?.compliance_flag)
    ? (raw.compliance_flag === "null" ? null : raw.compliance_flag)
    : heuristic.compliance_flag;

  const positive_signals = Array.isArray(raw?.positive_signals)
    ? raw.positive_signals.filter((s) => VALID_SIGNALS.has(s))
    : heuristic.positive_signals;

  const confidence = typeof raw?.confidence === "number"
    ? Math.max(0, Math.min(1, raw.confidence))
    : heuristic.confidence;

  const notes = typeof raw?.notes === "string" ? raw.notes.slice(0, 200) : "";

  return { language, objection, emotion, stage_hint, compliance_flag, positive_signals, confidence, notes };
}

async function aiAssistClassification({ message, heuristic_result }) {
  const safe_message = sanitizeForPrompt(message);

  const prompt = `You are classifying an inbound SMS from a real-estate property owner or contact.

<message>${safe_message}</message>

<heuristic_baseline>
${JSON.stringify(heuristic_result, null, 2)}
</heuristic_baseline>

Return ONLY valid JSON. No markdown. No explanation outside the JSON object.

{
  "language": "English|Spanish|Portuguese|Italian|Hebrew|Mandarin|Korean|Vietnamese|Polish|Arabic|Hindi|French|Russian|Japanese|Farsi|German|Greek|Thai|Pashto",
  "objection": "wrong_number|who_is_this|not_interested|already_listed|need_more_money|need_time|need_family_ok|send_offer_first|tenant_issue|condition_bad|probate|divorce|financial_distress|has_other_buyer|wants_retail|needs_call|needs_email|wants_written_offer|wants_proof_of_funds|null",
  "emotion": "calm|skeptical|guarded|frustrated|curious|motivated|tired_landlord|overwhelmed|grieving",
  "stage_hint": "Ownership|Offer|Q/A|Contract|Follow-Up",
  "compliance_flag": "stop_texting|null",
  "positive_signals": [],
  "confidence": 0.0,
  "notes": "one sentence explanation max"
}

Rules:
1. Trust the heuristic baseline unless the message clearly suggests a better classification.
2. If message is short, ambiguous, or a single word, lower confidence below 0.75.
3. Always preserve the detected language. If message uses non-English, keep that language.
4. compliance_flag takes absolute priority — when in any doubt, flag stop_texting.
5. positive_signals is an array of zero or more valid keys from the schema above.
6. Return objection and compliance_flag as the string "null" when not applicable.
7. Never invent fields not listed in the schema above.`;

  try {
    const response = await ai({
      model:       "gpt-4o-mini",
      temperature: 0,
      max_tokens:  400,
      messages:    [{ role: "user", content: prompt }],
    });

    const text = response?.choices?.[0]?.message?.content?.trim() ?? "";

    // Strip markdown code fences if model wraps in them
    const json_text = text.startsWith("```")
      ? text.replace(/^```(?:json)?\n?/, "").replace(/\n?```$/, "").trim()
      : text;

    const parsed = JSON.parse(json_text);
    return sanitizeAiResult(parsed, heuristic_result);
  } catch {
    // AI failed or returned malformed JSON — fall back to heuristic silently
    return null;
  }
}

// ══════════════════════════════════════════════════════════════════════════
// MAIN EXPORT
// ══════════════════════════════════════════════════════════════════════════

/**
 * classify(message, brain_item)
 *
 * Returns a fully-resolved classification result. Strategy:
 *   1. Always run heuristic synchronously — fast, zero-cost, no latency.
 *   2. If heuristic confidence < AI_CONFIDENCE_THRESHOLD, run AI assist.
 *   3. If AI returns a valid result, merge it; otherwise keep heuristic.
 *   4. Re-compute motivation_score on the final merged result so it
 *      always reflects the final objection + emotion, not the heuristic's.
 *
 * The compliance_flag is NEVER downgraded — if either heuristic or AI
 * sets it, it stays set in the final result.
 */

const AI_CONFIDENCE_THRESHOLD = 0.82;

export async function classify(message, brain_item = null) {
  const text = cleanMessage(message);

  if (!text) {
    return {
      language:        "English",
      objection:       null,
      emotion:         "guarded",
      stage_hint:      "Ownership",
      compliance_flag: null,
      positive_signals: [],
      confidence:      0.50,
      motivation_score: 50,
      source:          "heuristic",
      notes:           "",
    };
  }

  const heuristic = classifyHeuristic(text, brain_item);

  // Compliance is absolute — never route to AI for stop requests
  if (heuristic.compliance_flag === "stop_texting") {
    return {
      ...heuristic,
      motivation_score: estimateMotivationScore({
        objection:       heuristic.objection,
        emotion:         heuristic.emotion,
        positive_signals: heuristic.positive_signals,
      }),
      source: "heuristic",
      notes:  "",
    };
  }

  // High-confidence heuristic — skip AI call
  if (heuristic.confidence >= AI_CONFIDENCE_THRESHOLD) {
    return {
      ...heuristic,
      motivation_score: estimateMotivationScore({
        objection:       heuristic.objection,
        emotion:         heuristic.emotion,
        positive_signals: heuristic.positive_signals,
      }),
      source: "heuristic",
      notes:  "",
    };
  }

  // Low-confidence — call AI assist
  const ai_result = await aiAssistClassification({
    message:          text,
    heuristic_result: heuristic,
  });

  if (!ai_result) {
    // AI failed — return heuristic with fallback note
    return {
      ...heuristic,
      motivation_score: estimateMotivationScore({
        objection:       heuristic.objection,
        emotion:         heuristic.emotion,
        positive_signals: heuristic.positive_signals,
      }),
      source: "heuristic",
      notes:  "ai_assist_failed",
    };
  }

  // Merge: compliance can only be promoted, never demoted
  const final_compliance = heuristic.compliance_flag ?? ai_result.compliance_flag;

  const merged = {
    language:         ai_result.language,
    objection:        ai_result.objection,
    emotion:          ai_result.emotion,
    stage_hint:       ai_result.stage_hint,
    compliance_flag:  final_compliance,
    positive_signals: ai_result.positive_signals,
    confidence:       ai_result.confidence,
  };

  return {
    ...merged,
    motivation_score: estimateMotivationScore({
      objection:        merged.objection,
      emotion:          merged.emotion,
      positive_signals: merged.positive_signals,
    }),
    source: "ai",
    notes:  ai_result.notes ?? "",
  };
}

export default classify;