"""
enrich_fb_mapping.py — One-time enrichment of leagues.json with football.com league data.
Parses fb_region_leagues.html, matches to flashscore leagues, injects fb_ keys.
"""

import json
import re
import os
import string
from datetime import datetime, timezone
from bs4 import BeautifulSoup

# ═══════════════════════════════════════════════════════════════════════
# PATHS
# ═══════════════════════════════════════════════════════════════════════
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HTML_PATH = os.path.join(BASE, "Data", "Store", "fb_region_leagues.html")
JSON_PATH = os.path.join(BASE, "Data", "Store", "leagues.json")
AUDIT_PATH = os.path.join(BASE, "docs", "fb_leagues_mapping_audit.json")

# ═══════════════════════════════════════════════════════════════════════
# STEP 1 — PARSE fb_region_leagues.html
# ═══════════════════════════════════════════════════════════════════════

def normalize(name):
    """Lowercase, strip punctuation, collapse whitespace."""
    name = name.lower()
    name = name.translate(str.maketrans("", "", string.punctuation))
    name = re.sub(r"\s+", " ", name).strip()
    return name

def to_slug(name):
    """Convert name to URL slug: lowercase, replace spaces with hyphens, strip punctuation."""
    name = name.lower().strip()
    name = re.sub(r"\s+", "-", name)
    name = re.sub(r"[^\w\-]", "", name)
    name = re.sub(r"-+", "-", name)
    return name

def parse_html(path):
    """Parse fb_region_leagues.html and extract all league entries with hrefs."""
    with open(path, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f.read(), "html.parser")
    
    fb_leagues = []
    countries_set = set()
    
    league_blocks = soup.find_all("div", class_="m-league")
    
    for block in league_blocks:
        title_div = block.find("div", class_="m-league-title")
        if not title_div:
            continue
        
        text_span = title_div.find("span", class_="text")
        if not text_span:
            continue
        raw_country = text_span.get_text(strip=True)
        # Collapse any embedded newlines/whitespace from multiline HTML
        raw_country = re.sub(r"\s+", " ", raw_country).strip()
        
        # Skip "Top Leagues" — entries have no hrefs/category/tournament IDs
        if "Top" in raw_country and "League" in raw_country:
            continue
        
        country = re.sub(r"\d+$", "", raw_country).strip()
        countries_set.add(country)
        
        content_div = block.find("div", class_="m-league-conent")
        if not content_div:
            continue
        
        for li in content_div.find_all("li"):
            a_tag = li.find("a")
            if not a_tag:
                continue
            
            href = a_tag.get("href", "")
            if not href:
                continue
            
            name_div = a_tag.find("div", class_="m-item-left")
            if not name_div:
                continue
            league_name = name_div.get_text(strip=True)
            
            right_div = a_tag.find("div", class_="m-item-right")
            active_matches = 0
            if right_div:
                span = right_div.find("span")
                text = span.get_text(strip=True) if span else right_div.get_text(strip=True)
                try:
                    active_matches = int(text)
                except ValueError:
                    active_matches = 0
            
            cat_match = re.search(r"sr:category:(\d+)", href)
            tourn_match = re.search(r"sr:(?:simple_)?tournament:(\d+)", href)
            
            if not cat_match or not tourn_match:
                print(f"  [WARN] Could not parse IDs from href: {href}")
                continue
            
            cat_id = int(cat_match.group(1))
            tourn_id = int(tourn_match.group(1))
            
            fb_url = (
                f"https://www.football.com/ng/m/sport/football/"
                f"sr:category:{cat_id}/sr:tournament:{tourn_id}/"
                f"?time=all&source=sport_menu&sort=2"
            )
            
            fb_leagues.append({
                "fb_country": country,
                "fb_league_name": league_name,
                "fb_category_id": cat_id,
                "fb_tournament_id": tourn_id,
                "fb_url": fb_url,
                "fb_active_matches": active_matches,
            })
    
    # Deduplicate by tournament_id (same league can appear in country header + A-Z section)
    seen_tourn = {}
    deduped = []
    for league in fb_leagues:
        tid = league["fb_tournament_id"]
        if tid not in seen_tourn:
            seen_tourn[tid] = league
            deduped.append(league)
    
    return deduped, countries_set

# ═══════════════════════════════════════════════════════════════════════
# STEP 2 — BUILD COUNTRY BRIDGE
# ═══════════════════════════════════════════════════════════════════════

EXPLICIT_BRIDGE = {
    "England": "england",
    "Germany": "germany",
    "Spain": "spain",
    "France": "france",
    "Italy": "italy",
    "Netherlands": "netherlands",
    "Portugal": "portugal",
    "Turkey": "turkey",
    "Belgium": "belgium",
    "Scotland": "scotland",
    "Brazil": "brazil",
    "Argentina": "argentina",
    "USA": "usa",
    "Japan": "japan",
    "China": "china",
    "South Korea": "south-korea",
    "Saudi Arabia": "saudi-arabia",
    "UAE": "uae",
    "Russia": "russia",
    "Ukraine": "ukraine",
    "Poland": "poland",
    "Czech Republic": "czech-republic",
    "Austria": "austria",
    "Switzerland": "switzerland",
    "Greece": "greece",
    "Denmark": "denmark",
    "Sweden": "sweden",
    "Norway": "norway",
    "Finland": "finland",
    "Romania": "romania",
    "Hungary": "hungary",
    "Serbia": "serbia",
    "Croatia": "croatia",
    "Slovakia": "slovakia",
    "Slovenia": "slovenia",
    "Bulgaria": "bulgaria",
    "Israel": "israel",
    "Cyprus": "cyprus",
    "Ireland": "ireland",
    "Wales": "wales",
    "Northern Ireland": "northern-ireland",
    "Mexico": "mexico",
    "Colombia": "colombia",
    "Chile": "chile",
    "Peru": "peru",
    "Uruguay": "uruguay",
    "Ecuador": "ecuador",
    "Venezuela": "venezuela",
    "Paraguay": "paraguay",
    "Bolivia": "bolivia",
    "Nigeria": "nigeria",
    "South Africa": "south-africa",
    "Egypt": "egypt",
    "Morocco": "morocco",
    "Tunisia": "tunisia",
    "Algeria": "algeria",
    "Ghana": "ghana",
    "Kenya": "kenya",
    "Senegal": "senegal",
    "Cameroon": "cameroon",
    "Australia": "australia",
    "India": "india",
    "Indonesia": "indonesia",
    "Thailand": "thailand",
    "Malaysia": "malaysia",
    "Vietnam": "vietnam",
    "Iran": "iran",
    "Qatar": "qatar",
    "Kuwait": "kuwait",
    "Bahrain": "bahrain",
    "Jordan": "jordan",
    "Iraq": "iraq",
    "International": None,
    "Europe": None,
    "International Clubs": None,
    "International Youth": None,
    "Turkiye": "turkey",
    "Turkiye Amateur": None,
    "Czechia": "czech-republic",
    "Republic of Korea": "south-korea",
    "Hong Kong, China": "hong-kong",
    "Chinese Taipei": "chinese-taipei",
    "Simulated Reality League": None,
    "SoccerSpecials": None,
    "Denmark Amateur": None,
    "Spain Amateur": None,
    "England Amateur": None,
    "Austria Amateur": None,
    "Germany Amateur": None,
}

def build_country_slug_index(leagues_json):
    """Build mapping: country_slug → list of leagues.json entries."""
    slug_index = {}
    for entry in leagues_json:
        url = entry.get("url", "")
        m = re.search(r"/football/([^/]+)/", url)
        if m:
            slug = m.group(1)
            slug_index.setdefault(slug, []).append(entry)
    return slug_index

def build_country_bridge(fb_countries, slug_index):
    """Map fb_country names to flashscore country_slugs."""
    bridge = {}
    unresolved = []
    
    for country in fb_countries:
        if country in EXPLICIT_BRIDGE:
            bridge[country] = EXPLICIT_BRIDGE[country]
        else:
            candidate = country.lower().replace("&", "and").replace("  ", " ").replace(" ", "-")
            candidate = re.sub(r"[^a-z0-9\-]", "", candidate)
            candidate = re.sub(r"-+", "-", candidate).strip("-")
            
            if candidate in slug_index:
                bridge[country] = candidate
            else:
                unresolved.append(country)
    
    return bridge, unresolved


# ═══════════════════════════════════════════════════════════════════════
# STEP 3 — LEAGUE NAME MATCHING
# ═══════════════════════════════════════════════════════════════════════

ALIAS_TABLE = [
    # (fb_name, country_constraint_or_None, fs_name)
    ("Bundesliga", "Germany", "Bundesliga"),
    ("2. Bundesliga", "Germany", "2. Bundesliga"),
    ("3. Liga", "Germany", "3. Liga"),
    ("Primera Division", "Spain", "LaLiga"),
    ("Segunda Division", "Spain", "LaLiga2"),
    ("Serie A", "Italy", "Serie A"),
    ("Serie B", "Italy", "Serie B"),
    ("Ligue 1", "France", "Ligue 1"),
    ("Ligue 2", "France", "Ligue 2"),
    ("Eredivisie", "Netherlands", "Eredivisie"),
    ("Eerste Divisie", "Netherlands", "Eerste Divisie"),
    ("Jupiler Pro League", "Belgium", "Jupiler Pro League"),
    ("Pro League", "Belgium", "Jupiler Pro League"),
    ("Brasileiro Serie A", "Brazil", "Serie A Betano"),
    ("Brasileiro Serie B", "Brazil", "Serie B Superbet"),
    ("Brasileiro Serie C", "Brazil", "Serie C"),
    ("Brasileiro Serie D", "Brazil", "Serie D"),
    ("Super Lig", "Turkey", "Super Lig"),
    ("Premiership", "Scotland", "Premiership"),
    ("Championship", "Scotland", "Championship"),
    ("Ekstraklasa", "Poland", "Ekstraklasa"),
    ("Fortuna Liga", None, "Fortuna Liga"),
    ("UEFA Champions League", None, "Champions League"),
    ("UEFA Europa League", None, "Europa League"),
    ("UEFA Europa Conference League", None, "Conference League"),
    ("UEFA Conference League", None, "Conference League"),
    ("AFC Champions League Elite", None, "AFC Champions League"),
    ("J1 League", "Japan", "J1 League"),
    ("J2 League", "Japan", "J2 League"),
    ("K League 1", "South Korea", "K League 1"),
    ("K League 2", "South Korea", "K League 2"),
    ("Chinese Super League", "China", "Super League"),
    ("Primera LPF", "Argentina", "Liga Profesional"),
    ("MLS", "USA", "Major League Soccer"),
    ("EFL Cup", "England", "League Cup"),
    ("FA Cup", "England", "FA Cup"),
    ("EFL Trophy", "England", "EFL Trophy"),
    ("DFB-Pokal", "Germany", "DFB Pokal"),
    ("DFB Pokal", "Germany", "DFB Pokal"),
    ("Coppa Italia", "Italy", "Coppa Italia"),
    ("Copa del Rey", "Spain", "Copa del Rey"),
    ("Coupe de France", "France", "Coupe de France"),
    ("KNVB Cup", "Netherlands", "KNVB Beker"),
    ("Scottish Cup", "Scotland", "Scottish Cup"),
    ("Scottish League Cup", "Scotland", "Scottish League Cup"),
    ("LALIGA HYPERMOTION", "Spain", "LaLiga2"),
    ("CONMEBOL Libertadores", None, "Copa Libertadores"),
    ("CONCACAF Champions Cup", None, "CONCACAF Champions League"),
]

LIGA_1_BY_COUNTRY = {
    "Czech Republic": "Fortuna Liga",
    "Slovakia": "Fortuna Liga",
}

INTL_NAME_MAP = {
    "UEFA Champions League": "Champions League",
    "UEFA Europa League": "Europa League",
    "UEFA Europa Conference League": "Conference League",
    "UEFA Conference League": "Conference League",
    "UEFA Nations League": "Nations League",
    "FIFA World Cup": "World Cup",
    "AFC Champions League Elite": "AFC Champions League",
    "CONMEBOL Libertadores": "Copa Libertadores",
    "CONCACAF Champions Cup": "CONCACAF Champions League",
}

def match_leagues(fb_leagues, leagues_json, country_bridge, slug_index):
    """Match each fb league to a leagues.json entry using tiered approach."""
    
    # Build flashscore lookup structures
    fs_by_country_norm = {}   # (country_slug, norm_name) → list of entries
    fs_by_country_slug = {}   # (country_slug, league_slug) → list of entries
    fs_by_norm_name = {}      # norm_name → list of entries (global)
    
    for entry in leagues_json:
        url = entry.get("url", "")
        m = re.search(r"/football/([^/]+)/([^/]+)/?", url)
        if m:
            cs = m.group(1)
            ls = m.group(2)
            
            norm_name = normalize(entry["name"])
            fs_by_country_norm.setdefault((cs, norm_name), []).append(entry)
            fs_by_country_slug.setdefault((cs, ls), []).append(entry)
        
        norm_name = normalize(entry["name"])
        fs_by_norm_name.setdefault(norm_name, []).append(entry)
    
    matched = []      # (fb_entry, fs_entry, tier)
    review = []       # (fb_entry, fs_entry, tier, score)
    unmatched = []    # fb_entry
    
    for fb in fb_leagues:
        fb_country = fb["fb_country"]
        fb_name = fb["fb_league_name"]
        fb_name_norm = normalize(fb_name)
        
        country_slug = country_bridge.get(fb_country, "__UNRESOLVED__")
        is_intl = country_slug is None
        
        if country_slug == "__UNRESOLVED__":
            unmatched.append(fb)
            continue
        
        found = None
        tier = None
        
        # ── STEP 5: International / UEFA / FIFA ──
        if is_intl:
            if fb_name in INTL_NAME_MAP:
                target = normalize(INTL_NAME_MAP[fb_name])
                candidates = fs_by_norm_name.get(target, [])
                if candidates:
                    found = candidates[0]
                    tier = "T3"
            
            if not found:
                # Try partial name matching for intl
                for fs_entry in leagues_json:
                    fs_norm = normalize(fs_entry["name"])
                    if fb_name == "AFC Champions League Elite" and "afc champions" in fs_norm:
                        found = fs_entry
                        tier = "T3"
                        break
                    elif fb_name == "CONMEBOL Libertadores" and "copa libertadores" in fs_norm:
                        found = fs_entry
                        tier = "T3"
                        break
                    elif fb_name == "CONCACAF Champions Cup" and "concacaf champions" in fs_norm:
                        found = fs_entry
                        tier = "T3"
                        break
            
            if not found:
                unmatched.append(fb)
            else:
                matched.append((fb, found, tier))
            continue
        
        # ── TIER 1: Exact normalized name match ──
        key1 = (country_slug, fb_name_norm)
        candidates = fs_by_country_norm.get(key1, [])
        if len(candidates) >= 1:
            found = candidates[0]
            tier = "T1"
        
        # ── TIER 2: URL slug match ──
        if not found:
            fb_slug = to_slug(fb_name)
            key2 = (country_slug, fb_slug)
            candidates = fs_by_country_slug.get(key2, [])
            if len(candidates) >= 1:
                found = candidates[0]
                tier = "T2"
        
        # ── TIER 3: Known alias table ──
        if not found:
            for alias_fb, alias_country, alias_fs in ALIAS_TABLE:
                if fb_name == alias_fb:
                    if alias_country is None or alias_country == fb_country:
                        alias_norm = normalize(alias_fs)
                        key3 = (country_slug, alias_norm)
                        candidates = fs_by_country_norm.get(key3, [])
                        if candidates:
                            found = candidates[0]
                            tier = "T3"
                            break
                        # For international aliases (already handled above, but just in case)
                        if alias_country is None:
                            candidates = fs_by_norm_name.get(alias_norm, [])
                            if candidates:
                                found = candidates[0]
                                tier = "T3"
                                break
            
            # Special handling for "1. Liga"
            if not found and fb_name == "1. Liga" and fb_country in LIGA_1_BY_COUNTRY:
                target = normalize(LIGA_1_BY_COUNTRY[fb_country])
                key3 = (country_slug, target)
                candidates = fs_by_country_norm.get(key3, [])
                if candidates:
                    found = candidates[0]
                    tier = "T3"
        
        # ── TIER 4: Token overlap ──
        if not found:
            fb_tokens = set(fb_name_norm.split())
            best_score = 0
            best_candidate = None
            
            country_entries = slug_index.get(country_slug, [])
            for fs_entry in country_entries:
                fs_tokens = set(normalize(fs_entry["name"]).split())
                if not fb_tokens or not fs_tokens:
                    continue
                shared = fb_tokens & fs_tokens
                score = len(shared) / max(len(fb_tokens), len(fs_tokens))
                if score > best_score:
                    best_score = score
                    best_candidate = fs_entry
            
            if best_score >= 0.75 and best_candidate:
                review.append((fb, best_candidate, "T4", best_score))
                continue
        
        # ── TIER 5: No match ──
        if not found:
            unmatched.append(fb)
        else:
            matched.append((fb, found, tier))
    
    return matched, review, unmatched


# ═══════════════════════════════════════════════════════════════════════
# STEP 4 + 6 + 7 — INJECT, VERIFY, SAVE
# ═══════════════════════════════════════════════════════════════════════

def main():
    print("=" * 70)
    print("FB-to-Flashscore League Enrichment")
    print("=" * 70)
    
    # ── Step 1: Parse HTML ──
    fb_leagues, fb_countries = parse_html(HTML_PATH)
    n_leagues = len(fb_leagues)
    n_countries = len(fb_countries)
    print(f"\n[PARSE] Extracted {n_leagues} fb leagues across {n_countries} countries")
    
    if n_leagues != 330:
        print(f"[WARN] Expected 330 leagues but got {n_leagues}.")
        print(f"       (Top Leagues section excluded — it has no hrefs/IDs)")
    
    # ── Step 2: Load leagues.json and build country bridge ──
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        leagues_json = json.load(f)
    
    print(f"[LOAD] Loaded {len(leagues_json)} flashscore leagues from leagues.json")
    
    slug_index = build_country_slug_index(leagues_json)
    print(f"[INDEX] Built slug index with {len(slug_index)} country slugs")
    
    country_bridge, unresolved_countries = build_country_bridge(fb_countries, slug_index)
    resolved = {c for c, s in country_bridge.items() if s is not None}
    intl = {c for c, s in country_bridge.items() if s is None}
    print(f"[BRIDGE] Resolved {len(resolved)} countries, {len(intl)} international, {len(unresolved_countries)} unresolved")
    
    if unresolved_countries:
        print(f"[BRIDGE] Unresolved countries: {unresolved_countries}")
    
    # ── Step 3 + 5: Match leagues ──
    matched, review, unmatched = match_leagues(fb_leagues, leagues_json, country_bridge, slug_index)
    
    t1 = [(fb, fs, t) for fb, fs, t in matched if t == "T1"]
    t2 = [(fb, fs, t) for fb, fs, t in matched if t == "T2"]
    t3 = [(fb, fs, t) for fb, fs, t in matched if t == "T3"]
    
    print(f"\n{'=' * 70}")
    print(f"MATCHING RESULTS")
    print(f"{'=' * 70}")
    print(f"  Tier 1 (exact name):    {len(t1)}")
    print(f"  Tier 2 (URL slug):      {len(t2)}")
    print(f"  Tier 3 (alias table):   {len(t3)}")
    print(f"  Tier 4 (review req.):   {len(review)}")
    print(f"  Unmatched:              {len(unmatched)}")
    print(f"  TOTAL:                  {len(t1) + len(t2) + len(t3) + len(review) + len(unmatched)}")
    
    # ── Step 6: Verification ──
    print(f"\n{'=' * 70}")
    print(f"VERIFICATION")
    print(f"{'=' * 70}")
    
    # Duplicate tournament_id check
    tourn_ids_used = {}
    dup_conflicts = 0
    for fb, fs, tier in matched:
        tid = fb["fb_tournament_id"]
        if tid in tourn_ids_used:
            prev = tourn_ids_used[tid]
            print(f"[DUP] tournament_id {tid} used by BOTH:")
            print(f"       {prev[0]['fb_country']}/{prev[0]['fb_league_name']} -> {prev[1]['name']}")
            print(f"       {fb['fb_country']}/{fb['fb_league_name']} -> {fs['name']}")
            dup_conflicts += 1
        else:
            tourn_ids_used[tid] = (fb, fs)
    
    # Country consistency check
    country_mismatches = 0
    for fb, fs, tier in matched:
        expected_slug = country_bridge.get(fb["fb_country"])
        if expected_slug is None:
            continue
        fs_url = fs.get("url", "")
        if expected_slug not in fs_url:
            print(f"[COUNTRY-MISMATCH] {fb['fb_country']}/{fb['fb_league_name']} -> {fs['name']} URL={fs_url}")
            country_mismatches += 1
    
    print(f"\n  Duplicate tournament_id conflicts: {dup_conflicts}")
    print(f"  Country mismatches: {country_mismatches}")
    
    # Spot-check
    spot_check_names = [
        ("England", "Premier League"),
        ("Germany", "Bundesliga"),
        ("Spain", "LaLiga"),
        ("Italy", "Serie A"),
        ("France", "Ligue 1"),
        ("International Clubs", "UEFA Champions League"),
        ("International Clubs", "UEFA Europa League"),
        ("Netherlands", "Eredivisie"),
        ("Brazil", "Brasileiro Serie A"),
        ("Japan", "J1 League"),
    ]
    
    print(f"\n{'=' * 70}")
    print(f"SPOT-CHECK (10 well-known leagues)")
    print(f"{'=' * 70}")
    
    match_dict = {(fb["fb_country"], fb["fb_league_name"]): (fb, fs, tier) for fb, fs, tier in matched}
    
    for sc_country, sc_name in spot_check_names:
        key = (sc_country, sc_name)
        if key in match_dict:
            fb, fs, tier = match_dict[key]
            print(f"  OK [{tier}] {sc_country}/{sc_name}")
            print(f"    fb_url:  {fb['fb_url']}")
            print(f"    fs_url:  {fs['url']}")
            print(f"    fs_name: {fs['name']}")
        else:
            found_in_alt = False
            for fb2, fs2, tier2 in matched:
                if fb2["fb_league_name"] == sc_name:
                    print(f"  ~  [{tier2}] {sc_name} found under {fb2['fb_country']} (expected {sc_country})")
                    print(f"    fb_url:  {fb2['fb_url']}")
                    print(f"    fs_url:  {fs2['url']}")
                    found_in_alt = True
                    break
            if not found_in_alt:
                print(f"  FAIL {sc_country}/{sc_name} -- NOT MATCHED")
    
    # ── Step 4: Inject fb_ keys ──
    inject_map = {}
    
    for fb, fs, tier in matched:
        lid = fs["league_id"]
        if lid in inject_map:
            print(f"[WARN] Multiple fb leagues mapped to same fs league_id: {lid}")
            continue
        inject_map[lid] = {
            "fb_league_name": fb["fb_league_name"],
            "fb_country": fb["fb_country"],
            "fb_category_id": str(fb["fb_category_id"]),
            "fb_tournament_id": str(fb["fb_tournament_id"]),
            "fb_url": fb["fb_url"],
            "fb_active_matches": fb["fb_active_matches"],
            "fb_matched_tier": tier,
        }
    
    injected_count = 0
    for entry in leagues_json:
        lid = entry["league_id"]
        if lid in inject_map:
            entry.update(inject_map[lid])
            injected_count += 1
    
    print(f"\n[INJECT] Added fb_ keys to {injected_count} leagues.json entries")
    
    # ── Step 7: Save ──
    if dup_conflicts > 0:
        print(f"\n[BLOCKER] {dup_conflicts} duplicate tournament_id conflicts found!")
        print(f"          Fix these before saving. Aborting save.")
        return
    
    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(leagues_json, f, indent=4, ensure_ascii=False)
    print(f"[SAVE] Updated leagues.json saved to {JSON_PATH}")
    
    # Build audit
    audit = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_fb_leagues": n_leagues,
        "matched_t1": len(t1),
        "matched_t2": len(t2),
        "matched_t3": len(t3),
        "review_required": len(review),
        "unmatched_count": len(unmatched),
        "duplicate_conflicts": dup_conflicts,
        "country_mismatches": country_mismatches,
        "unresolved_countries": unresolved_countries,
        "matches": [],
        "unmatched": [],
        "review_required_list": [],
    }
    
    for fb, fs, tier in matched:
        audit["matches"].append({
            "fb_country": fb["fb_country"],
            "fb_league_name": fb["fb_league_name"],
            "fb_tournament_id": str(fb["fb_tournament_id"]),
            "fb_category_id": str(fb["fb_category_id"]),
            "fb_url": fb["fb_url"],
            "fs_league_id": fs["league_id"],
            "fs_name": fs["name"],
            "fs_url": fs["url"],
            "tier": tier,
            "confidence": "CONFIRMED",
        })
    
    for fb in unmatched:
        audit["unmatched"].append({
            "fb_country": fb["fb_country"],
            "fb_league_name": fb["fb_league_name"],
            "fb_category_id": str(fb["fb_category_id"]),
            "fb_tournament_id": str(fb["fb_tournament_id"]),
        })
    
    for fb, fs, tier, score in review:
        audit["review_required_list"].append({
            "fb_country": fb["fb_country"],
            "fb_league_name": fb["fb_league_name"],
            "fb_tournament_id": str(fb["fb_tournament_id"]),
            "fb_url": fb["fb_url"],
            "fs_league_id": fs["league_id"],
            "fs_name": fs["name"],
            "fs_url": fs["url"],
            "tier": tier,
            "confidence": "REVIEW_REQUIRED",
            "match_score": round(score, 4),
        })
    
    os.makedirs(os.path.dirname(AUDIT_PATH), exist_ok=True)
    with open(AUDIT_PATH, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=4, ensure_ascii=False)
    print(f"[SAVE] Audit file saved to {AUDIT_PATH}")
    
    # ── Final Summary ──
    print(f"\n{'=' * 70}")
    print(f"FINAL SUMMARY")
    print(f"{'=' * 70}")
    print(f"  [RESULT] Total fb leagues in HTML:          {n_leagues}")
    print(f"  [RESULT] Successfully matched (T1):         {len(t1)}")
    print(f"  [RESULT] Successfully matched (T2):         {len(t2)}")
    print(f"  [RESULT] Successfully matched (T3):         {len(t3)}")
    print(f"  [RESULT] Matched pending review (T4):       {len(review)}")
    print(f"  [RESULT] Unmatched (no fb_ keys added):     {len(unmatched)}")
    print(f"  [RESULT] Duplicate tournament_id conflicts: {dup_conflicts}")
    print(f"  [RESULT] Country bridge failures:           {len(unresolved_countries)}")
    if unresolved_countries:
        for c in unresolved_countries:
            print(f"           -> {c}")
    
    if unmatched:
        print(f"\n{'=' * 70}")
        print(f"UNMATCHED LIST ({len(unmatched)})")
        print(f"{'=' * 70}")
        for fb in unmatched:
            print(f"  [UNMATCHED] {fb['fb_country']} / {fb['fb_league_name']} (cat:{fb['fb_category_id']} tourn:{fb['fb_tournament_id']})")
    
    if review:
        print(f"\n{'=' * 70}")
        print(f"REVIEW_REQUIRED LIST ({len(review)})")
        print(f"{'=' * 70}")
        for fb, fs, tier, score in review:
            print(f"  [REVIEW] {fb['fb_country']}/{fb['fb_league_name']} -> matched to \"{fs['name']}\" (score: {score:.2f})")
    
    print(f"\n{'=' * 70}")
    print(f"DONE")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
