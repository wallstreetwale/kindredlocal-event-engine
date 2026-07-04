require('dotenv').config();
const express = require('express');
const axios = require('axios');
const cron = require('node-cron');

const app = express();
const PORT = process.env.PORT || 8080;

// ── API Keys ──────────────────────────────────────────────────────────────────
const SERPAPI_KEY = process.env.SERPAPI_KEY;
const TICKETMASTER_KEY = process.env.TICKETMASTER_KEY;
const OPENAI_KEY = process.env.OPENAI_KEY;
const KINDREDLOCAL_API_URL = process.env.KINDREDLOCAL_API_URL;
const KINDREDLOCAL_ADMIN_TOKEN = process.env.KINDREDLOCAL_ADMIN_TOKEN;

// Security config (set both in Railway env vars)
const ENGINE_ADMIN_KEY = process.env.ENGINE_ADMIN_KEY;
const APP_ORIGIN = process.env.APP_ORIGIN || '';

app.use(express.json());

// Scoped CORS — only your app origin, not '*'
app.use((req, res, next) => {
  if (APP_ORIGIN) res.header('Access-Control-Allow-Origin', APP_ORIGIN);
  res.header('Access-Control-Allow-Headers', 'Authorization, Content-Type, x-engine-key');
  next();
});

// Shared-secret gate for sensitive routes
function requireEngineKey(req, res, next) {
  const key = req.headers['x-engine-key'] || '';
  if (!ENGINE_ADMIN_KEY || key !== ENGINE_ADMIN_KEY) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  next();
}

// ── City Configuration ──────────────────────────────────────────────────────
const CITIES = [
  { name: 'Austin', state: 'TX', query: 'Austin TX', zip: '78701', active: true },
  { name: 'Minneapolis', state: 'MN', query: 'Minneapolis MN', zip: '55401', active: true },
  { name: 'Dallas', state: 'TX', query: 'Dallas TX', zip: '75201', active: false },
  { name: 'Houston', state: 'TX', query: 'Houston TX', zip: '77001', active: false },
  { name: 'Orlando', state: 'FL', query: 'Orlando FL', zip: '32801', active: false },
  { name: 'Jacksonville', state: 'FL', query: 'Jacksonville FL', zip: '32099', active: false },
  { name: 'Oakland', state: 'CA', query: 'Oakland CA', zip: '94601', active: false },
  { name: 'San Francisco', state: 'CA', query: 'San Francisco CA', zip: '94102', active: false },
  { name: 'Chicago', state: 'IL', query: 'Chicago IL', zip: '60601', active: false },
  { name: 'Atlanta', state: 'GA', query: 'Atlanta GA', zip: '30301', active: false },
  { name: 'Charlotte', state: 'NC', query: 'Charlotte NC', zip: '28201', active: false },
  { name: 'Phoenix', state: 'AZ', query: 'Phoenix AZ', zip: '85001', active: false },
];

const SEARCH_QUERIES = [
  'family friendly events',
  'kids activities',
  'family events this weekend',
  'children activities',
  'family outdoor activities',
  'faith family events',
  'homeschool family events',
  'free family events',
];

const ALLOWED_CATEGORIES = [
  'Learning & Education', 'Sports & Movement', 'Arts & Creativity',
  'Social & Community', 'Nature & Outdoors', 'Culture & History', 'Faith & Values',
];

// ── Engine State ──────────────────────────────────────────────────────────────
let engineState = {
  lastRun: null,
  totalDiscovered: 0,
  totalSubmitted: 0,
  totalRejected: 0,
  cityStats: {},
  recentEvents: [],
  errors: [],
};

// ── Expiry (replaces the old delete-all cleanup) ─────────────────────────────
// Calls Base44's expireStaleActivities, which deletes ONLY past-dated events
// (with a grace window) and never touches admin_locked / recurring / future events.
async function expireStaleActivities() {
  if (!KINDREDLOCAL_API_URL || !KINDREDLOCAL_ADMIN_TOKEN) {
    console.log('[EXPIRE] Skipped — KindredLocal API not configured');
    return 0;
  }
  try {
    const r = await axios.post(
      `${KINDREDLOCAL_API_URL}/api/functions/expireStaleActivities`,
      {},
      { headers: { Authorization: `Bearer ${KINDREDLOCAL_ADMIN_TOKEN}`, 'Content-Type': 'application/json' }, timeout: 15000 }
    );
    const expired = r.data?.expired ?? r.data?.deleted ?? 0;
    console.log(`[EXPIRE] Removed ${expired} past-dated events`);
    return expired;
  } catch (err) {
    console.error('[EXPIRE] Failed:', err.message);
    return 0;
  }
}

// ── SerpApi — Google Events Search ───────────────────────────────────────────
async function searchGoogleEvents(query, city) {
  try {
    console.log(`SerpApi: searching "${query}" in ${city.name}`);
    const response = await axios.get('https://serpapi.com/search', {
      params: {
        engine: 'google_events',
        q: `${query} ${city.query}`,
        location: `${city.name}, ${city.state}`,
        hl: 'en',
        gl: 'us',
        api_key: SERPAPI_KEY,
      },
      timeout: 15000,
    });

    const events = response.data.events_results || [];
    console.log(`SerpApi: found ${events.length} events for "${query}" in ${city.name}`);

    return events.map(event => ({
      source: 'google_events',
      title: event.title || '',
      date: event.date?.start_date || event.date?.when || '',
      time: event.date?.when || '',
      address: Array.isArray(event.address) ? event.address.join(', ') : event.address || '',
      venue: event.venue?.name || '',
      description: event.description || '',
      link: event.link || '',
      thumbnail: event.thumbnail || '',
      city: city.name,
      state: city.state,
      rawData: event,
    }));
  } catch (err) {
    console.error(`SerpApi error for ${city.name}:`, err.message);
    engineState.errors.push({ source: 'serpapi', city: city.name, error: err.message, timestamp: new Date().toISOString() });
    return [];
  }
}

// ── Ticketmaster — Family Events ──────────────────────────────────────────────
async function searchTicketmasterEvents(city) {
  try {
    console.log(`Ticketmaster: searching family events in ${city.name}`);
    const startDate = new Date().toISOString().split('.')[0] + 'Z';
    const endDate = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString().split('.')[0] + 'Z';

    const response = await axios.get('https://app.ticketmaster.com/discovery/v2/events.json', {
      params: {
        apikey: TICKETMASTER_KEY,
        city: city.name,
        stateCode: city.state,
        classificationName: 'Family',
        startDateTime: startDate,
        endDateTime: endDate,
        size: 20,
        sort: 'date,asc',
      },
      timeout: 15000,
    });

    const events = response.data._embedded?.events || [];
    console.log(`Ticketmaster: found ${events.length} family events in ${city.name}`);

    return events.map(event => ({
      source: 'ticketmaster',
      title: event.name || '',
      date: event.dates?.start?.localDate || '',
      time: event.dates?.start?.localTime || '',
      address: event._embedded?.venues?.[0]?.address?.line1 || '',
      venue: event._embedded?.venues?.[0]?.name || '',
      description: event.info || event.pleaseNote || '',
      link: event.url || '',
      thumbnail: event.images?.[0]?.url || '',
      priceMin: event.priceRanges?.[0]?.min || null,
      priceMax: event.priceRanges?.[0]?.max || null,
      city: city.name,
      state: city.state,
      rawData: event,
    }));
  } catch (err) {
    console.error(`Ticketmaster error for ${city.name}:`, err.message);
    engineState.errors.push({ source: 'ticketmaster', city: city.name, error: err.message, timestamp: new Date().toISOString() });
    return [];
  }
}

// ── OpenAI — Safety Classifier (safety separate from quality) ─────────────────
async function filterAndClassifyEvent(event) {
  try {
    const prompt = `You are a child-safety and content classifier for KindredLocal, a family activity app for faith-forward families with young children.

Analyze this event and respond with ONLY a valid JSON object, no other text.

Event Title: ${event.title}
Description: ${event.description || 'No description'}
Venue: ${event.venue || 'Unknown'}
Date: ${event.date || 'Unknown'}
City: ${event.city}, ${event.state}

Return exactly this JSON:
{
  "family_safe": true or false,
  "safety_confidence": number 0-100,
  "safety_flags": [array of strings],
  "safety_reason": "one sentence explaining the family_safe judgment (ALWAYS fill this)",
  "min_appropriate_age": number 0-18,
  "quality_confidence": number 0-100,
  "category": one of exactly: "Learning & Education","Sports & Movement","Arts & Creativity","Social & Community","Nature & Outdoors","Culture & History","Faith & Values",
  "ageRange": "All ages" or "Toddlers (0-3)" or "Kids (4-8)" or "Tweens (9-12)" or "Teens (13-17)" or "Adults",
  "isFree": true or false or null,
  "cleanTitle": improved title, max 60 chars,
  "cleanDescription": family-friendly description, max 200 chars,
  "faithRelevant": true or false
}

RULES:
- Judge "family_safe" on the CONTENT and SETTING, not on how well-written the listing is. A sparse description is a quality issue, not a safety issue.
- Set "family_safe": false for: adult/mature content, alcohol- or cannabis-centered events, gambling, violence, events at 21+ venues (bars/clubs/breweries) even if billed as "family night," late-night adult events, political rallies, or anything a faith-forward parent would not want a child exposed to.
- Use "safety_flags" for softer concerns that do NOT auto-reject but a human should see: e.g. "alcohol_served","venue_21plus","late_night","mature_themes","unverified_organizer","crowd_18plus".
- Set "safety_confidence" BELOW 90 whenever there is any real ambiguity. Bias toward human review, never toward passing.
- "faithRelevant": true for church events, faith communities, worship, prayer, religious celebrations, or moral/character development.
- "quality_confidence" reflects how complete/clear the listing is (separate from safety).`;

    const response = await axios.post(
      'https://api.openai.com/v1/chat/completions',
      {
        model: 'gpt-4o-mini',
        messages: [{ role: 'user', content: prompt }],
        max_tokens: 400,
        temperature: 0.1,
      },
      {
        headers: { Authorization: `Bearer ${OPENAI_KEY}`, 'Content-Type': 'application/json' },
        timeout: 15000,
      }
    );

    const content = response.data.choices[0].message.content.trim();
    const clean = content.replace(/```json|```/g, '').trim();
    const parsed = JSON.parse(clean);
    return validateClassifierOutput(parsed);
  } catch (err) {
    console.error(`OpenAI filter error for "${event.title}":`, err.message);
    // Never return null (that silently drops). Signal a failure the caller routes to review.
    return { classifier_failed: true, reason: `classifier error: ${err.message}` };
  }
}

// Validate + coerce classifier output before trusting it.
function validateClassifierOutput(o) {
  if (!o || typeof o !== 'object') {
    return { classifier_failed: true, reason: 'classifier returned non-object' };
  }
  if (typeof o.family_safe !== 'boolean') {
    return { classifier_failed: true, reason: 'family_safe not a boolean' };
  }
  let sc = Number(o.safety_confidence);
  if (!Number.isFinite(sc)) sc = 0;
  o.safety_confidence = Math.max(0, Math.min(100, sc));

  let qc = Number(o.quality_confidence);
  if (!Number.isFinite(qc)) qc = 0;
  o.quality_confidence = Math.max(0, Math.min(100, qc));

  if (!ALLOWED_CATEGORIES.includes(o.category)) {
    o.category = 'Social & Community';
    o.safety_flags = Array.isArray(o.safety_flags) ? o.safety_flags : [];
    o.safety_flags.push('category_defaulted');
    o.safety_confidence = Math.min(o.safety_confidence, 89);
  }

  let age = Number(o.min_appropriate_age);
  if (!Number.isFinite(age) || age < 0 || age > 18) {
    o.min_appropriate_age = null;
    o.safety_flags = Array.isArray(o.safety_flags) ? o.safety_flags : [];
    o.safety_flags.push('age_unclear');
  }
  if (!Array.isArray(o.safety_flags)) o.safety_flags = [];
  if (typeof o.safety_reason !== 'string' || !o.safety_reason) {
    o.safety_reason = 'no reason provided';
  }
  return o;
}

// ── Duplicate detection (within a single run only) ───────────────────────────
// Cross-run dedup is handled by Base44's external_id upsert, so this set is
// cleared at the start of every run.
const processedTitles = new Set();
function isDuplicate(event) {
  const key = `${event.title.toLowerCase().replace(/\s+/g, '')}|${event.city}|${event.date}`;
  if (processedTitles.has(key)) return true;
  processedTitles.add(key);
  return false;
}

// ── Stable external_id for upsert ────────────────────────────────────────────
// Key off venue + date (stable across title wording) so the same real event
// discovered under slightly different titles collapses to ONE record on upsert.
// Falls back to title only when a venue isn't available.
function makeExternalId(event) {
  const norm = (s) =>
    String(s || '').toLowerCase().replace(/\s+/g, '-').replace(/[^a-z0-9:_-]/g, '');

  const cityPart = norm(event.city);
  const datePart = norm(event.date);
  const venuePart = norm(event.venue);

  if (venuePart) {
    return `${event.source}:${cityPart}:${venuePart}:${datePart}`.slice(0, 200);
  }
  const titlePart = norm(event.title);
  return `${event.source}:${cityPart}:${titlePart}:${datePart}`.slice(0, 200);
}

// ── Submit to KindredLocal (sends safety fields + external_id + split address) ─
async function submitToKindredLocal(event, aiResult, status) {
  try {
    const activity = {
      external_id: makeExternalId(event),
      name: aiResult.cleanTitle || event.title,
      description: aiResult.cleanDescription || event.description,
      category: aiResult.category,
      location_name: event.venue || '',
      location_address: event.address || '',
      city: event.city,
      state: event.state,
      date_type: event.date ? 'specific' : 'recurring',
      specific_date: event.date || null,
      start_time: event.time || null,
      is_free: aiResult.isFree,
      age_range: aiResult.ageRange,
      min_appropriate_age: aiResult.min_appropriate_age ?? null,
      external_link: event.link || null,
      thumbnail: event.thumbnail || null,
      source: `AI Discovered — ${event.source}`,
      // safety signal — the whole point of Fix 6
      ai_confidence: aiResult.quality_confidence ?? null,
      family_safe: aiResult.family_safe ?? null,
      safety_confidence: aiResult.safety_confidence ?? null,
      safety_reason: aiResult.safety_reason ?? null,
      safety_flags: aiResult.safety_flags ?? [],
      faith_relevant: aiResult.faithRelevant,
      status, // routed by caller; Base44 backstop re-enforces
      submitter_name: 'KindredLocal Event Engine',
      submitter_email: 'engine@kindredlocal.com',
      auto_discovered: true,
    };

    if (KINDREDLOCAL_API_URL && KINDREDLOCAL_ADMIN_TOKEN) {
      await axios.post(
        `${KINDREDLOCAL_API_URL}/api/functions/submitDiscoveredActivity`,
        activity,
        {
          headers: {
            Authorization: `Bearer ${KINDREDLOCAL_ADMIN_TOKEN}`,
            'Content-Type': 'application/json',
          },
          timeout: 10000,
        }
      );
      console.log(`✓ Submitted "${activity.name}" (${event.city}) status=${status} safety_conf=${activity.safety_confidence}`);
    } else {
      console.log(`[TEST MODE] Would submit "${activity.name}" (${event.city}) status=${status}`);
    }
    return true;
  } catch (err) {
    console.error(`Submit error for "${event.title}":`, err.message);
    return false;
  }
}

// ── Process a single city ─────────────────────────────────────────────────────
async function processCity(city) {
  console.log(`\n=== Processing ${city.name}, ${city.state} ===`);

  if (!engineState.cityStats[city.name]) {
    engineState.cityStats[city.name] = { discovered: 0, submitted: 0, rejected: 0, lastRun: null };
  }

  const allEvents = [];
  const queriesToRun = SEARCH_QUERIES.slice(0, 2);
  for (const query of queriesToRun) {
    const events = await searchGoogleEvents(query, city);
    allEvents.push(...events);
    await new Promise(r => setTimeout(r, 1000));
  }

  const tmEvents = await searchTicketmasterEvents(city);
  allEvents.push(...tmEvents);

  console.log(`${city.name}: ${allEvents.length} raw events found`);
  engineState.cityStats[city.name].discovered += allEvents.length;
  engineState.totalDiscovered += allEvents.length;

  let submitted = 0;
  let rejected = 0;

  for (const event of allEvents) {
    if (!event.title || event.title.length < 3) continue;

    if (isDuplicate(event)) {
      console.log(`Duplicate skipped: "${event.title}"`);
      continue;
    }

    const aiResult = await filterAndClassifyEvent(event);
    await new Promise(r => setTimeout(r, 500));

    // Route — never silently drop, never silently pass.
    let routedStatus;
    if (aiResult.classifier_failed) {
      routedStatus = 'pending_review';
      aiResult.safety_reason = `classifier failed — needs manual review (${aiResult.reason || 'unknown'})`;
      aiResult.safety_flags = ['classifier_failed'];
      aiResult.family_safe = null;
      aiResult.safety_confidence = null;
      aiResult.quality_confidence = aiResult.quality_confidence ?? null;
      aiResult.category = aiResult.category || 'Social & Community';
      aiResult.cleanTitle = aiResult.cleanTitle || event.title;
      aiResult.cleanDescription = aiResult.cleanDescription || event.description;
      aiResult.ageRange = aiResult.ageRange || 'All ages';
      aiResult.isFree = aiResult.isFree ?? null;
      aiResult.faithRelevant = aiResult.faithRelevant ?? false;
    } else if (aiResult.family_safe === false) {
      routedStatus = 'rejected_safety';
    } else if (aiResult.family_safe === true && aiResult.safety_confidence >= 90) {
      routedStatus = 'approved';
    } else {
      routedStatus = 'pending_review';
    }

    const success = await submitToKindredLocal(event, aiResult, routedStatus);
    if (success) {
      if (routedStatus === 'rejected_safety') {
        rejected++;
        engineState.totalRejected++;
      } else {
        submitted++;
        engineState.totalSubmitted++;
        engineState.recentEvents.unshift({
          title: aiResult.cleanTitle || event.title,
          city: event.city,
          category: aiResult.category,
          status: routedStatus,
          safety_confidence: aiResult.safety_confidence,
          faithRelevant: aiResult.faithRelevant,
          timestamp: new Date().toISOString(),
        });
        if (engineState.recentEvents.length > 50) engineState.recentEvents.pop();
      }
    }

    await new Promise(r => setTimeout(r, 200));
  }

  engineState.cityStats[city.name].submitted += submitted;
  engineState.cityStats[city.name].rejected += rejected;
  engineState.cityStats[city.name].lastRun = new Date().toISOString();

  console.log(`${city.name} complete: ${submitted} submitted, ${rejected} rejected`);
}

// ── Main Discovery Run ────────────────────────────────────────────────────────
async function runDiscovery(citiesOverride = null) {
  console.log('\n====================================');
  console.log('KindredLocal Event Discovery Engine');
  console.log(`Started: ${new Date().toISOString()}`);
  console.log('====================================\n');

  // Fresh dedup set each run; cross-run dedup handled by Base44 upsert.
  processedTitles.clear();

  // Expire past-dated events (never wipes current/future/locked ones).
  await expireStaleActivities();

  const citiesToProcess = citiesOverride || CITIES.filter(c => c.active);

  for (const city of citiesToProcess) {
    try {
      await processCity(city);
      await new Promise(r => setTimeout(r, 3000));
    } catch (err) {
      console.error(`Error processing ${city.name}:`, err.message);
      engineState.errors.push({ city: city.name, error: err.message, timestamp: new Date().toISOString() });
    }
  }

  engineState.lastRun = new Date().toISOString();

  console.log('\n====================================');
  console.log(`Discovery complete: ${new Date().toISOString()}`);
  console.log(`Total discovered: ${engineState.totalDiscovered}`);
  console.log(`Total submitted: ${engineState.totalSubmitted}`);
  console.log(`Total rejected: ${engineState.totalRejected}`);
  console.log('====================================\n');
}

// ── Cron — Daily at 6AM CST (11AM UTC) ───────────────────────────────────────
cron.schedule('0 11 * * *', () => {
  console.log('Cron triggered: daily discovery run');
  runDiscovery();
});

// ── Routes ────────────────────────────────────────────────────────────────────
app.get('/health', (req, res) => {
  res.json({ status: 'running' });
});

app.get('/status', requireEngineKey, (req, res) => {
  res.json({ ...engineState, cities: CITIES });
});

app.get('/run', requireEngineKey, async (req, res) => {
  res.json({ message: 'Discovery run started', timestamp: new Date().toISOString() });
  runDiscovery();
});

app.get('/run/:city', requireEngineKey, async (req, res) => {
  const city = CITIES.find(c => c.name.toLowerCase() === req.params.city.toLowerCase());
  if (!city) return res.status(404).json({ error: 'City not found' });
  res.json({ message: `Running discovery for ${city.name}`, timestamp: new Date().toISOString() });
  runDiscovery([city]);
});

app.get('/recent', requireEngineKey, (req, res) => {
  res.json({ count: engineState.recentEvents.length, events: engineState.recentEvents });
});

app.get('/cities', requireEngineKey, (req, res) => {
  res.json(CITIES);
});

// ── Start Server ──────────────────────────────────────────────────────────────
app.listen(PORT, '0.0.0.0', () => {
  console.log(`KindredLocal Event Discovery Engine running on port ${PORT}`);
  console.log(`APIs: SerpApi=${!!SERPAPI_KEY} Ticketmaster=${!!TICKETMASTER_KEY} OpenAI=${!!OPENAI_KEY} KindredLocal=${!!KINDREDLOCAL_API_URL}`);
  console.log(`Engine key set: ${!!ENGINE_ADMIN_KEY} | App origin: ${APP_ORIGIN || '(none)'}`);
  console.log(`Cities active: ${CITIES.filter(c => c.active).length}/${CITIES.length}`);
});
