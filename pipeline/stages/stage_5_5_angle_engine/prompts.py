"""
prompts.py — LLM prompt templates for all 10 RSOC angle types.

All prompts target qwen3:14b via Ollama.
Variables use {keyword}, {vertical}, {language}, {year}, {audience},
{discovery_context_note}, {spanish_instruction} — must be populated by
build_prompt() before sending to the model.

{spanish_instruction} is now populated from LANGUAGE_INSTRUCTIONS for ALL
supported languages, not just Spanish. The variable name is kept for backward
compatibility with all 10 prompt templates.

RAF compliance is baked into every prompt.
Source: spec Section 4.2, grounded in 22 confirmed live RSOC articles.
"""

# Per-language instruction blocks injected into every prompt via {spanish_instruction}.
# Variable name kept for backward compatibility with prompt templates.
LANGUAGE_INSTRUCTIONS = {
    "es": """
LANGUAGE REQUIREMENTS (SPANISH):
- Write entirely in formal US Spanish (use "usted" register for legal/medical verticals)
- Use proper diacritical marks: á, é, í, ó, ú, ñ, ü — do NOT strip accents
- For legal verticals: use "abogado," "reclamación," "daños," "negligencia" — not anglicisms
- For medical verticals: use "cobertura," "beneficiario," "medicamento"
- Avoid overly regional vocabulary; use neutral US Spanish readable across Mexican,
  Puerto Rican, Central American, and Dominican Spanish-speaking populations
- Output: clean Markdown with proper H1/H2 formatting, all text in Spanish""",

    "fr": """
LANGUAGE REQUIREMENTS (FRENCH):
- Write entirely in formal French (vouvoiement — use "vous" throughout)
- Use proper diacritical marks: à, â, é, è, ê, ë, î, ï, ô, ù, û, ü, ç — do NOT strip accents
- Prefer neutral French over Québécois or Belgian regional expressions
- Output: clean Markdown with proper H1/H2 formatting, all text in French""",

    "de": """
LANGUAGE REQUIREMENTS (GERMAN):
- Write entirely in formal German (use "Sie" form throughout)
- Use correct umlauts and ß: ä, ö, ü, Ä, Ö, Ü, ß — do NOT substitute ae/oe/ue
- Prefer neutral standard German (Hochdeutsch) over Austrian or Swiss regional expressions
- Output: clean Markdown with proper H1/H2 formatting, all text in German""",

    "pt": """
LANGUAGE REQUIREMENTS (PORTUGUESE):
- Write entirely in formal Portuguese
- Use proper diacritical marks: ã, â, á, à, é, ê, í, ó, ô, õ, ú, ç — do NOT strip accents
- If country is BR: Brazilian Portuguese register; if PT: European Portuguese register
- Output: clean Markdown with proper H1/H2 formatting, all text in Portuguese""",

    "it": """
LANGUAGE REQUIREMENTS (ITALIAN):
- Write entirely in formal Italian (use "Lei" form for formal address)
- Use proper diacritical marks: à, è, é, ì, í, î, ò, ó, ù, ú — do NOT strip accents
- Output: clean Markdown with proper H1/H2 formatting, all text in Italian""",

    "nl": """
LANGUAGE REQUIREMENTS (DUTCH):
- Write entirely in formal Dutch (use "u" form throughout)
- Use correct Dutch spelling including ij/IJ digraphs — do NOT convert to y
- Prefer neutral standard Dutch over Flemish regional expressions
- Output: clean Markdown with proper H1/H2 formatting, all text in Dutch""",

    "pl": """
LANGUAGE REQUIREMENTS (POLISH):
- Write entirely in formal Polish (use Pan/Pani forms for address)
- Preserve all Polish diacritical marks: ą, ć, ę, ł, ń, ó, ś, ź, ż — do NOT strip them
- Output: clean Markdown with proper H1/H2 formatting, all text in Polish""",

    "ja": """
LANGUAGE REQUIREMENTS (JAPANESE):
- Write entirely in Japanese using natural, fluent prose
- Use polite/formal register (丁寧語, -ます/-です forms) throughout
- Mix kanji, hiragana, and katakana naturally — do NOT write in romaji
- Use full-width punctuation: 。、「」 — not ASCII punctuation
- Output: clean Markdown with proper H1/H2 formatting, all text in Japanese""",

    "ko": """
LANGUAGE REQUIREMENTS (KOREAN):
- Write entirely in Korean using natural, fluent prose
- Use formal polite register (합쇼체 — -습니다/-ㅂ니다 endings) throughout
- Do NOT romanize — write entirely in Hangul with appropriate Hanja if needed
- Use Korean punctuation conventions
- Output: clean Markdown with proper H1/H2 formatting, all text in Korean""",

    "zh": """
LANGUAGE REQUIREMENTS (CHINESE):
- Write entirely in Simplified Chinese (简体中文)
- Use natural, fluent Mandarin prose — neutral Putonghua register
- Do NOT use Traditional Chinese characters unless country is TW or HK
- Use full-width punctuation: 。，、：；「」（）— not ASCII punctuation
- Output: clean Markdown with proper H1/H2 formatting, all text in Chinese""",

    "ar": """
LANGUAGE REQUIREMENTS (ARABIC):
- Write entirely in Modern Standard Arabic (الفصحى / MSA)
- Text flows right-to-left — ensure no Latin characters appear in the body text
- Use proper Arabic punctuation: ، ؟ ؛
- Use proper diacritical marks (tashkeel) where needed for clarity in technical terms
- Output: clean Markdown with proper H1/H2 formatting, all text in Arabic""",

    "vi": """
LANGUAGE REQUIREMENTS (VIETNAMESE):
- Write entirely in formal Vietnamese
- Preserve all tone marks and diacritical marks: ắ, ặ, ầ, ẩ, ề, ệ, ộ, ợ, ừ, ứ, etc.
- Use formal "Quý vị" or "bạn" address as appropriate for the topic register
- Output: clean Markdown with proper H1/H2 formatting, all text in Vietnamese""",

    "th": """
LANGUAGE REQUIREMENTS (THAI):
- Write entirely in Thai script — do NOT romanize or transliterate
- Use formal/polite Thai register (ภาษาสุภาพ) throughout
- Thai prose typically does not use spaces between words — follow natural Thai conventions
- Output: clean Markdown with proper H1/H2 formatting, all text in Thai""",

    "el": """
LANGUAGE REQUIREMENTS (GREEK):
- Write entirely in Modern Greek (Standard Modern Greek / Νέα Ελληνική)
- Use formal register (ενεστώτας οριστικής, formal address)
- Preserve all Greek diacritical marks: accent marks (τόνος) on all polysyllabic words
- Output: clean Markdown with proper H1/H2 formatting, all text in Greek""",

    "sv": """
LANGUAGE REQUIREMENTS (SWEDISH):
- Write entirely in formal Swedish
- Use neutral standard Swedish (rikssvenska) — avoid regional dialects
- Preserve Swedish special characters: å, ä, ö, Å, Ä, Ö — do NOT substitute them
- Output: clean Markdown with proper H1/H2 formatting, all text in Swedish""",

    "tr": """
LANGUAGE REQUIREMENTS (TURKISH):
- Write entirely in formal Turkish
- Use formal address ("Siz" form) throughout
- Preserve Turkish special characters: ç, ğ, ı, İ, ö, ş, ü — do NOT substitute them
- Output: clean Markdown with proper H1/H2 formatting, all text in Turkish""",
}

# Backward-compat alias
SPANISH_INSTRUCTION = LANGUAGE_INSTRUCTIONS["es"]

ANGLE_PROMPTS = {}

ANGLE_PROMPTS["eligibility_explainer"] = """You are writing an informational article for a content publishing website.
This article must be genuinely useful to readers while covering a commercial topic.
Do NOT sell anything. Do NOT include calls to action. Do NOT mention ads or sponsored content.

ARTICLE TYPE: Eligibility explainer
KEYWORD: {keyword}
VERTICAL: {vertical}
LANGUAGE: {language}
YEAR: {year}
TARGET AUDIENCE: {audience}

REQUIRED STRUCTURE:
H1 title: Use this exact title — "{title}"

Opening paragraph (80-150 words): Immediately introduce the topic. In the first 3 sentences,
include: the keyword phrase, one process/eligibility term (such as "qualify," "requirements,"
"criteria," or "eligibility"), and one contextual anchor (such as "in {year}" or "for many people").
Do NOT start with "Are you wondering" or any question hook.

H2 sections (write 4-5 of these, 150-250 words each):
- "What {keyword} Eligibility Typically Requires"
- "How Qualifying Factors Are Evaluated in {year}"
- "Who Is Generally Considered Eligible for {keyword}"
- "Common Questions About {keyword} Qualification"
- "Next Steps for Those Exploring {keyword} Options" (closing)

TONE: Neutral, informational, third-person where possible. Authoritative but not promotional.
WORD COUNT: 700-1,100 words total.
RAF COMPLIANCE: Do not promise outcomes. Do not use "you will receive" or "guaranteed."
Do not claim to offer legal, medical, or financial advice.{discovery_context_note}
{spanish_instruction}
Output the article in clean Markdown with proper H1/H2 formatting."""

ANGLE_PROMPTS["how_it_works_explainer"] = """You are writing an informational article for a content publishing website.
This article must be genuinely educational. Never sell. Never recommend specific providers.

ARTICLE TYPE: How-it-works / structural explainer
KEYWORD: {keyword}
VERTICAL: {vertical}
LANGUAGE: {language}
YEAR: {year}
TARGET AUDIENCE: {audience}

REQUIRED STRUCTURE:
H1 title: Use this exact title — "{title}"

Opening paragraph (80-150 words): Begin with a factual observation about the process or system.
Include the keyword, a process term (such as "how it works," "structured," "administered," or "handled"),
and a year reference. Signal that the article will explain a process, not sell a product.

H2 sections (4-5 sections):
- "How the {keyword} Process Typically Unfolds"
- "What Drives Variation in {keyword} Outcomes"
- "Key Components of the {keyword} System"
- "What People Often Get Wrong About {keyword}"
- "What to Expect When Navigating {keyword}" (closing)

TONE: Explanatory. Like a knowledgeable friend explaining a system — not a salesperson.
WORD COUNT: 750-1,100 words.
RAF COMPLIANCE: No specific legal, medical, or financial advice. No price claims without "typically" or "generally."{discovery_context_note}
{spanish_instruction}
Output in clean Markdown."""

ANGLE_PROMPTS["trend_attention_piece"] = """You are writing an informational trend article for a content publishing website.
Frame the topic as a newsworthy subject gaining attention — not a promotion.

ARTICLE TYPE: Trend / attention piece
KEYWORD: {keyword}
VERTICAL: {vertical}
LANGUAGE: {language}
YEAR: {year}
TARGET AUDIENCE: {audience}

REQUIRED STRUCTURE:
H1 title: Use this exact title — "{title}"

Opening paragraph: Establish that interest in the topic has grown. Include the keyword,
a trend signal ("growing," "increasing attention," "more people are exploring"),
and a {year} reference. Do not fabricate statistics.

H2 sections (4-5):
- "Why Interest in {keyword} Has Increased in {year}"
- "What People Are Saying About {keyword}"
- "Factors That Drive Ongoing Attention to {keyword}"
- "How {keyword} Has Evolved Over Time"
- "What Observers Note About {keyword} Today" (closing)

TONE: Journalistic and observational. Describe the trend, not the product.
WORD COUNT: 700-1,000 words.
RAF COMPLIANCE: Do not endorse specific brands. Do not invent quotes or statistics.{discovery_context_note}
{spanish_instruction}
Output in clean Markdown."""

ANGLE_PROMPTS["lifestyle_fit_analysis"] = """You are writing a lifestyle-fit analysis article for a content publishing website.
Help readers understand whether a program or product suits their daily life.

ARTICLE TYPE: Lifestyle fit analysis
KEYWORD: {keyword}
VERTICAL: {vertical}
LANGUAGE: {language}
YEAR: {year}
TARGET AUDIENCE: {audience}

REQUIRED STRUCTURE:
H1 title: Use this exact title — "{title}"

Opening paragraph: Introduce the topic and establish that this article explores fit —
not whether to buy. Include keyword and a lifestyle term.

H2 sections (4-5):
- "What {audience} Typically Look For in {keyword}"
- "How {keyword} Aligns With Daily Priorities"
- "What Benefits Matter Most in {keyword} Situations"
- "When {keyword} Makes Practical Sense"
- "How to Evaluate Whether {keyword} Fits Your Needs" (closing)

TONE: Helpful, conversational, audience-aware. Write as if speaking to that audience directly.
WORD COUNT: 700-1,000 words.
RAF COMPLIANCE: No endorsements. No "you should sign up." Present information for the reader to decide.{discovery_context_note}
{spanish_instruction}
Output in clean Markdown."""

ANGLE_PROMPTS["pre_review_guide"] = """You are writing a pre-decision guide for a content publishing website.
The reader is cautious and wants to be prepared before exploring options. Never rush them.

ARTICLE TYPE: Pre-review / what-to-know guide
KEYWORD: {keyword}
VERTICAL: {vertical}
LANGUAGE: {language}
YEAR: {year}
TARGET AUDIENCE: {audience}

REQUIRED STRUCTURE:
H1 title: Use this exact title — "{title}"

Opening paragraph: Establish the reader's pre-decision mindset. Include the keyword, a caution signal
("before committing," "worth understanding," "key considerations"), and a {year} reference.

H2 sections (4-5):
- "What to Review Before Committing to {keyword}"
- "Common Questions Worth Asking About {keyword}"
- "What People Often Overlook When Evaluating {keyword}"
- "Red Flags to Watch for With {keyword} Options"
- "How to Approach the {keyword} Decision Process" (closing)

TONE: Cautious advocate — like a trusted advisor preparing someone before a big decision.
WORD COUNT: 700-1,100 words.
RAF COMPLIANCE: No specific provider recommendations. No "apply now" language.{discovery_context_note}
{spanish_instruction}
Output in clean Markdown."""

ANGLE_PROMPTS["policy_year_stamped_update"] = """You are writing a policy update article for a content publishing website.
Frame the topic around current-year context that affects the reader's situation.

ARTICLE TYPE: Policy review / year-stamped update
KEYWORD: {keyword}
VERTICAL: {vertical}
LANGUAGE: {language}
YEAR: {year}
TARGET AUDIENCE: {audience}

REQUIRED STRUCTURE:
H1 title: Use this exact title — "{title}"

Opening paragraph: Immediately establish that conditions related to the keyword have
relevance to {year}. Include keyword, year, and a change signal ("updated," "reform,"
"new rules," "current conditions"). Do not claim false urgency or fabricate changes.

H2 sections (4-5):
- "What Changed With {keyword} in {year}"
- "How Current {keyword} Conditions Affect Your Situation"
- "Key Updates to {keyword} Worth Knowing"
- "What Experts Are Observing About {year} {keyword} Developments"
- "How to Stay Informed About {keyword} Changes" (closing)

TONE: Timely, informative, objective. Like a reliable news briefing on a complex topic.
WORD COUNT: 750-1,100 words.
RAF COMPLIANCE: Only reference verifiable real-world policies. Do not fabricate specific legislation.
If uncertain, use hedged language ("some analysts suggest," "according to program documentation").{discovery_context_note}
{spanish_instruction}
Output in clean Markdown."""

ANGLE_PROMPTS["accusatory_expose"] = """You are writing a consumer-awareness article for a content publishing website.
Inform readers about information that is often not disclosed proactively in this industry.

ARTICLE TYPE: Consumer awareness / exposé framing
KEYWORD: {keyword}
VERTICAL: {vertical}
LANGUAGE: {language}
YEAR: {year}
TARGET AUDIENCE: {audience}

REQUIRED STRUCTURE:
H1 title: Use this exact title — "{title}"

Opening paragraph: Establish that the topic has important dimensions that aren't always
visible to consumers. Include keyword, a disclosure gap signal, and a neutral caution.
Do NOT accuse specific companies of wrongdoing. Use general industry framing.

H2 sections (4-5):
- "Information Not Always Volunteered in {keyword} Discussions"
- "How {keyword} Decisions Typically Affect Outcomes"
- "What the Details Say About {keyword} Situations"
- "Why Independent Research Matters With {keyword}"
- "Questions Worth Asking Before Proceeding With {keyword}" (closing)

TONE: Investigative but neutral. Advocate for the reader's awareness without making legal allegations.
RAF COMPLIANCE CRITICAL: Do NOT accuse specific named companies. Do NOT promise legal outcomes.
Do NOT use phrases like "you deserve compensation" or "you may be owed money."
WORD COUNT: 750-1,100 words.{discovery_context_note}
{spanish_instruction}
Output in clean Markdown."""

ANGLE_PROMPTS["hidden_costs"] = """You are writing a financial-awareness article for a content publishing website.
Help readers understand the full cost picture of a topic they are exploring.

ARTICLE TYPE: Hidden costs
KEYWORD: {keyword}
VERTICAL: {vertical}
LANGUAGE: {language}
YEAR: {year}
TARGET AUDIENCE: {audience}

REQUIRED STRUCTURE:
H1 title: Use this exact title — "{title}"

Opening paragraph: Establish that the keyword topic involves costs or financial considerations
that are not always immediately obvious. Include keyword, a cost signal, and a framing of
consumer awareness. Do NOT fabricate specific dollar amounts.

H2 sections (4-5):
- "Costs That Aren't Always Mentioned Upfront in {keyword} Situations"
- "How {keyword} Expenses Are Typically Calculated"
- "Factors That Affect the Total Cost of {keyword}"
- "How to Protect Yourself from Unexpected {keyword} Expenses"
- "Planning Ahead for {keyword} Financial Considerations" (closing)

TONE: Financial-awareness focused. Like a consumer advocate's briefing — factual, not alarmist.
WORD COUNT: 700-1,100 words.
RAF COMPLIANCE: No specific price claims without hedging ("typically," "can range"). No "get money back" language.{discovery_context_note}
{spanish_instruction}
Output in clean Markdown."""

ANGLE_PROMPTS["diagnostic_signs"] = """You are writing a situation-assessment article for a content publishing website.
Help readers understand whether their situation matches criteria for a particular type of assistance.

ARTICLE TYPE: Diagnostic / signs article
KEYWORD: {keyword}
VERTICAL: {vertical}
LANGUAGE: {language}
YEAR: {year}
TARGET AUDIENCE: {audience}

REQUIRED STRUCTURE:
H1 title: Use this exact title — "{title}"

Opening paragraph: Establish that many people are uncertain whether their situation
involves {keyword}. Include keyword, a situation-assessment signal, and a non-alarmist tone.
Do NOT tell readers they "definitely have a claim."

H2 sections (4-5):
- "Situations That Commonly Involve {keyword}"
- "What Typically Distinguishes a {keyword} Situation"
- "How Documentation Relates to {keyword} Cases"
- "When People Typically Seek Help with {keyword}"
- "How to Begin Evaluating a Potential {keyword} Situation" (closing)

TONE: Informative, empathetic, cautious. Do not advise. Do not diagnose.
RAF COMPLIANCE: This is NOT legal/medical advice. Never say "you have a case."
Use "may," "typically," "in some situations."
WORD COUNT: 700-1,100 words.{discovery_context_note}
{spanish_instruction}
Output in clean Markdown."""

ANGLE_PROMPTS["comparison"] = """You are writing a neutral comparison article for a content publishing website.
Present multiple options fairly without recommending one over the other.

ARTICLE TYPE: Comparison
KEYWORD: {keyword}
VERTICAL: {vertical}
LANGUAGE: {language}
YEAR: {year}
TARGET AUDIENCE: {audience}

REQUIRED STRUCTURE:
H1 title: Use this exact title — "{title}"

Opening paragraph: Establish the comparison context. Include the keyword, the audience,
and a {year} reference. Signal that this is a neutral overview.

H2 sections (4-5):
- "Key Differences to Consider With {keyword} Options"
- "What Each {keyword} Option Typically Offers"
- "Cost and Eligibility Considerations for Each Option"
- "How People Typically Decide Between {keyword} Choices"
- "What to Consider When Evaluating {keyword} Options" (closing)

TONE: Balanced and fair. Neither option is "better." Present factors; let the reader decide.
WORD COUNT: 750-1,100 words.
RAF COMPLIANCE: Do NOT recommend one option over the other.{discovery_context_note}
{spanish_instruction}
Output in clean Markdown."""


def build_prompt(
    angle_type: str,
    keyword: str,
    vertical: str,
    language_code: str,
    year: int,
    title: str,
    audience: str = "adults researching this topic",
    discovery_signal_text: str = "",
    source_trend: str = "",
    option_a: str = "",
    option_b: str = "",
) -> str:
    """
    Build the final LLM prompt string for a given angle.
    Injects all variables into the template.
    Returns the complete prompt ready to send to Ollama.
    """
    template = ANGLE_PROMPTS.get(angle_type)
    if not template:
        raise ValueError(f"Unknown angle_type: {angle_type}")

    # Discovery context note — grounded in actual trend text when available
    if source_trend and source_trend.strip():
        discovery_note = (
            "\n\nIMPORTANT — TREND CONTEXT:\n"
            "This keyword was derived from the following real-world signal:\n"
            f"\"{source_trend.strip()}\"\n"
            "Ground the opening paragraph in this context. Reference the "
            "underlying event or discussion naturally — do not copy the "
            "headline verbatim. Do not fabricate details beyond what the "
            "headline states."
        )
    elif discovery_signal_text:
        # Fallback: generic signal-type note (legacy behavior)
        context_map = {
            "google_trends":       "This topic is currently trending. Reflect that relevance in the opening paragraph.",
            "reddit_discussion":   "This keyword surfaced from a community discussion. Address the concerns real people raise.",
            "news_event":          "This keyword is tied to a recent development. Acknowledge its current significance without fabricating details.",
            "keyword_expansion":   "",
            "commercial_transform":"",
        }
        if "Trends" in discovery_signal_text or "trending" in discovery_signal_text.lower():
            note = context_map["google_trends"]
        elif "Reddit" in discovery_signal_text or "reddit" in discovery_signal_text.lower():
            note = context_map["reddit_discussion"]
        elif "News" in discovery_signal_text:
            note = context_map["news_event"]
        else:
            note = ""
        discovery_note = f"\n{note}" if note else ""
    else:
        discovery_note = ""

    spanish_instr = LANGUAGE_INSTRUCTIONS.get(language_code.lower(), "")

    return template.format(
        keyword=keyword,
        vertical=vertical,
        language=language_code.upper(),
        year=year,
        audience=audience,
        title=title,
        discovery_context_note=discovery_note,
        spanish_instruction=spanish_instr,
        option_a=option_a or keyword,
        option_b=option_b or f"{keyword} alternatives",
    )
