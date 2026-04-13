require('dotenv').config();
const express = require('express');
const axios = require('axios');
const cron = require('node-cron');
 
const app = express();
const PORT = process.env.PORT || 8080;
 
// API Keys from environment variables
const SERPAPI_KEY = process.env.SERPAPI_KEY;
const TICKETMASTER_KEY = process.env.TICKETMASTER_KEY;
const OPENAI_KEY = process.env.OPENAI_KEY;
const KINDREDLOCAL_API_URL = process.env.KINDREDLOCAL_API_URL;
const KINDREDLOCAL_ADMIN_TOKEN = process.env.KINDREDLOCAL_ADMIN_TOKEN;
 
app.use(express.json());
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Authorization, Content-Type');
  next();
});
 
// ── City Configuration ────────────────────────────────────────────────────────
const CITIES = [
  { name: 'Austin', state: 'TX', query: 'Austin TX', zip: '78701', active: true },
  { name: 'Dallas', state: 'TX', query: 'Dallas TX', zip: '75201', active: true },
  { name: 'Houston', state: 'TX', query: 'Houston TX', zip: '77001', active: true },
  { name: 'Orlando', state: 'FL', query: 'Orlando FL', zip: '32801', active: true },
  { name: 'Jacksonville', state: 'FL', query: 'Jacksonville FL', zip: '32099', active: true },
  { name: 'Oakland', state: 'CA', query: 'Oakland CA', zip: '94601', active: true },
  { name: 'San Francisco', state: 'CA', query: 'San Francisco CA', zip: '94102', active: true },
  { name: 'Chicago', state: 'IL', query: 'Chicago IL', zip: '60601', active: true },
  { name: 'Atlanta', state: 'GA', query: 'Atlanta GA', zip: '30301', active: true },
  { name: 'Charlotte', state: 'NC', query: 'Charlotte NC', zip: '28201', active: true },
  { name: 'Phoenix', state: 'AZ', query: 'Phoenix AZ', zip: '85001', active: true },
];
 
// ── KindredLocal Categories ───────────────────────────────────────────────────
const CATEGORIES = [
  'Learning & Education',
  'Sports & Movement',
  'Arts & Creativity',
  'Social & Community',
  'Nature & Outdoors',
  'Culture & History',
  'Faith & Values',
];
 
// ── Search Queries per city ───────────────────────────────────────────────────
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
 
    // Calculate date range — next 30 days
    const startDate = new Date().toISOString().split('.')[0] + 'Z';
    const endDate = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString().split('.')[0] + 'Z';
 
    const response = await axios.get('https://app.ticketmaster.com/discovery/v2/events.json', {
      params: {
        apikey: TICKETMASTER_KEY,
        city: city.name,
        stateCode: city.state,
        classificationName: 'Family', // Family category
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
 
// ── OpenAI — AI Filter and Classifier ────────────────────────────────────────
async function filterAndClassifyEvent(event) {
  try {
    const prompt = `You are a content filter for KindredLocal, a family activity app for faith-forward families with children. 
 
Analyze this event and respond with ONLY a valid JSON object, no other text:
 
Event Title: ${event.title}
Description: ${event.description || 'No description'}
Venue: ${event.venue || 'Unknown'}
Date: ${event.date || 'Unknown'}
City: ${event.city}, ${event.state}
 
Return this exact JSON structure:
{
  "appropriate": true or false,
  "confidence": number between 0 and 100,
  "category": one of exactly: "Learning & Education", "Sports & Movement", "Arts & Creativity", "Social & Community", "Nature & Outdoors", "Culture & History", "Faith & Values",
  "ageRange": "All ages" or "Toddlers (0-3)" or "Kids (4-8)" or "Tweens (9-12)" or "Teens (13-17)" or "Adults",
  "isFree": true or false or null,
  "cleanTitle": improved version of the title, max 60 characters,
  "cleanDescription": family-friendly description, max 200 characters,
  "faithRelevant": true or false,
  "rejectionReason": null or brief reason if not appropriate
}
 
Mark as NOT appropriate if: adult content, alcohol-focused, gambling, violence, political rallies, or clearly not family-friendly.
Mark faithRelevant as true if: church events, faith communities, religious celebrations, prayer, worship, or moral/character development.
Give confidence above 85 only if clearly family-friendly with good details.`;
 
    const response = await axios.post(
      'https://api.openai.com/v1/chat/completions',
      {
        model: 'gpt-4o-mini',
        messages: [{ role: 'user', content: prompt }],
        max_tokens: 300,
        temperature: 0.1,
      },
      {
        headers: {
          Authorization: `Bearer ${OPENAI_KEY}`,
          'Content-Type': 'application/json',
        },
        timeout: 15000,
      }
    );
 
    const content = response.data.choices[0].message.content.trim();
    const clean = content.replace(/```json|```/g, '').trim();
    return JSON.parse(clean);
  } catch (err) {
    console.error(`OpenAI filter error for "${event.title}":`, err.message);
    return null;
  }
}
 
// ── Duplicate Detection ───────────────────────────────────────────────────────
const processedTitles = new Set();
 
function isDuplicate(event) {
  // Create a normalized key from title + city + date
  const key = `${event.title.toLowerCase().replace(/\s+/g, '')}|${event.city}|${event.date}`;
  if (processedTitles.has(key)) return true;
  processedTitles.add(key);
  return false;
}
 
// ── Submit to KindredLocal Admin Queue ────────────────────────────────────────
async function submitToKindredLocal(event, aiResult) {
  try {
    // Format the event for KindredLocal's activity submission format
    const activity = {
      name: aiResult.cleanTitle || event.title,
      description: aiResult.cleanDescription || event.description,
      category: aiResult.category,
      location: `${event.venue ? event.venue + ', ' : ''}${event.address || ''}, ${event.city}, ${event.state}`,
      city: event.city,
      state: event.state,
      date_type: event.date ? 'specific' : 'recurring',
      specific_date: event.date || null,
      start_time: event.time || null,
      is_free: aiResult.isFree,
      age_range: aiResult.ageRange,
      external_link: event.link || null,
      thumbnail: event.thumbnail || null,
      source: `AI Discovered — ${event.source}`,
      ai_confidence: aiResult.confidence,
      faith_relevant: aiResult.faithRelevant,
      status: aiResult.confidence >= 85 ? 'pending_fast_approve' : 'pending_review',
      submitter_name: 'KindredLocal Event Engine',
      submitter_email: 'engine@kindredlocal.com',
      auto_discovered: true,
    };
 
    // Submit to KindredLocal API
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
      console.log(`✓ Submitted: "${activity.name}" (${event.city}) — confidence: ${aiResult.confidence}%`);
    } else {
      // Log for testing when KindredLocal API not connected
      console.log(`[TEST MODE] Would submit: "${activity.name}" (${event.city}) — confidence: ${aiResult.confidence}%`);
    }
 
    return true;
  } catch (err) {
    console.error(`Submit error for "${event.title}":`, err.message);
    return false;
  }
}
 
// ── Process Single City ───────────────────────────────────────────────────────
async function processCity(city) {
  console.log(`\n=== Processing ${city.name}, ${city.state} ===`);
 
  if (!engineState.cityStats[city.name]) {
    engineState.cityStats[city.name] = { discovered: 0, submitted: 0, rejected: 0, lastRun: null };
  }
 
  const allEvents = [];
 
  // Search Google Events with multiple queries
  // Use 2 queries per city to stay within SerpApi free tier
  const queriesToRun = SEARCH_QUERIES.slice(0, 2);
  for (const query of queriesToRun) {
    const events = await searchGoogleEvents(query, city);
    allEvents.push(...events);
    await new Promise(r => setTimeout(r, 1000)); // Rate limit
  }
 
  // Search Ticketmaster
  const tmEvents = await searchTicketmasterEvents(city);
  allEvents.push(...tmEvents);
 
  console.log(`${city.name}: ${allEvents.length} raw events found`);
  engineState.cityStats[city.name].discovered += allEvents.length;
  engineState.totalDiscovered += allEvents.length;
 
  // Process each event through AI filter
  let submitted = 0;
  let rejected = 0;
 
  for (const event of allEvents) {
    // Skip if no title
    if (!event.title || event.title.length < 3) continue;
 
    // Skip duplicates
    if (isDuplicate(event)) {
      console.log(`Duplicate skipped: "${event.title}"`);
      continue;
    }
 
    // AI filter and classify
    const aiResult = await filterAndClassifyEvent(event);
    await new Promise(r => setTimeout(r, 500)); // Rate limit OpenAI
 
    if (!aiResult) {
      rejected++;
      continue;
    }
 
    // Reject inappropriate or low confidence events
    if (!aiResult.appropriate || aiResult.confidence < 60) {
      console.log(`Rejected: "${event.title}" — ${aiResult.rejectionReason || 'low confidence'} (${aiResult.confidence}%)`);
      rejected++;
      engineState.totalRejected++;
      continue;
    }
 
    // Submit qualifying events
    const success = await submitToKindredLocal(event, aiResult);
    if (success) {
      submitted++;
      engineState.totalSubmitted++;
      engineState.recentEvents.unshift({
        title: aiResult.cleanTitle || event.title,
        city: event.city,
        category: aiResult.category,
        confidence: aiResult.confidence,
        faithRelevant: aiResult.faithRelevant,
        timestamp: new Date().toISOString(),
      });
      // Keep only last 50 recent events
      if (engineState.recentEvents.length > 50) engineState.recentEvents.pop();
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
 
  const citiesToProcess = citiesOverride || CITIES.filter(c => c.active);
 
  for (const city of citiesToProcess) {
    try {
      await processCity(city);
      // Delay between cities to respect rate limits
      await new Promise(r => setTimeout(r, 3000));
    } catch (err) {
      console.error(`Error processing ${city.name}:`, err.message);
      engineState.errors.push({
        city: city.name,
        error: err.message,
        timestamp: new Date().toISOString(),
      });
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
 
// ── Cron Schedule — Daily at 6AM CST (11AM UTC) ──────────────────────────────
cron.schedule('0 11 * * *', () => {
  console.log('Cron triggered: daily discovery run');
  runDiscovery();
});
 
// ── Express Routes ────────────────────────────────────────────────────────────
app.get('/health', (req, res) => {
  res.json({
    status: 'running',
    lastRun: engineState.lastRun,
    totalDiscovered: engineState.totalDiscovered,
    totalSubmitted: engineState.totalSubmitted,
    totalRejected: engineState.totalRejected,
    citiesConfigured: CITIES.length,
    citiesActive: CITIES.filter(c => c.active).length,
    apis: {
      serpapi: !!SERPAPI_KEY,
      ticketmaster: !!TICKETMASTER_KEY,
      openai: !!OPENAI_KEY,
      kindredlocal: !!KINDREDLOCAL_API_URL,
    },
  });
});
 
app.get('/status', (req, res) => {
  res.json({
    ...engineState,
    cities: CITIES,
  });
});
 
app.get('/run', async (req, res) => {
  res.json({ message: 'Discovery run started', timestamp: new Date().toISOString() });
  runDiscovery();
});
 
app.get('/run/:city', async (req, res) => {
  const cityName = req.params.city;
  const city = CITIES.find(c => c.name.toLowerCase() === cityName.toLowerCase());
  if (!city) {
    return res.status(404).json({ error: 'City not found', available: CITIES.map(c => c.name) });
  }
  res.json({ message: `Running discovery for ${city.name}`, timestamp: new Date().toISOString() });
  runDiscovery([city]);
});
 
app.get('/cities', (req, res) => {
  res.json(CITIES);
});
 
app.get('/recent', (req, res) => {
  res.json({
    count: engineState.recentEvents.length,
    events: engineState.recentEvents,
  });
});
 
// ── Start Server ──────────────────────────────────────────────────────────────
app.listen(PORT, '0.0.0.0', () => {
  console.log(`KindredLocal Event Discovery Engine running on port ${PORT}`);
  console.log(`APIs configured: SerpApi=${!!SERPAPI_KEY} Ticketmaster=${!!TICKETMASTER_KEY} OpenAI=${!!OPENAI_KEY}`);
  console.log(`Cities configured: ${CITIES.length}`);
  console.log(`Next scheduled run: Daily at 6AM CST`);
});
 
