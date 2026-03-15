# match_resolver.py: Team name resolution for Football.com match pairing.
# Part of LeoBook Modules — FootballCom
#
# Classes: GrokMatcher
# Cascade: search_dict (exact alias) → Gemini LLM fallback.
# Fuzzy removed (RULEBOOK §1 first principles — search_dict is sufficient
# once leagues are matched; fuzzy adds noise not accuracy).
# Grok removed (retired model, not cost-effective).

import os
import json
import sqlite3
import asyncio
from typing import List, Dict, Optional, Tuple, Set

_session_dead_models: Set[str] = set()

try:
    from google import genai
    from google.genai import types
    HAS_GEMINI = True
except ImportError:
    HAS_GEMINI = False

class GrokMatcher:
    """
    2-stage cascade resolver: search_dict (exact alias) → Gemini LLM.
    Fuzzy removed: once leagues are matched, search_dict aliases are
    sufficient. Fuzzy added noise, not accuracy (RULEBOOK §1 first principles).
    Imported by fb_manager.py for Chapter 1 Page 1 resolution.
    """

    def __init__(self):
        self._cache: Dict[str, Optional[Dict]] = {}

    @staticmethod
    def _get_name(m: Dict, role: str) -> str:
        """Extract home/away name from a candidate dict."""
        return (m.get(f'{role}_team') or m.get(role) or '').strip()

    def _get_team_id(self, m: Dict, role: str) -> Optional[str]:
        """Extract team_id for auto-learn storage."""
        return m.get(f'{role}_team_id') or m.get(f'{role}_id')

    def _get_search_terms(self, conn: sqlite3.Connection, team_id: Optional[str]) -> List[str]:
        """Load alternative search terms for a team from the search_dict table."""
        if not team_id or not conn:
            return []
        try:
            cur = conn.execute(
                "SELECT search_terms FROM search_dict WHERE team_id = ?", (team_id,)
            )
            row = cur.fetchone()
            if row and row[0]:
                return json.loads(row[0]) if isinstance(row[0], str) else row[0]
        except Exception:
            pass
        return []

    def _auto_learn(self, conn: sqlite3.Connection, team_id: Optional[str], new_alias: str) -> None:
        """Persist a newly discovered alias into search_dict for future sessions."""
        if not team_id or not conn or not new_alias:
            return
        try:
            alias_lower = new_alias.strip().lower()
            terms = self._get_search_terms(conn, team_id)
            if alias_lower not in [t.strip().lower() for t in terms]:
                terms.append(new_alias.strip())
                conn.execute(
                    "INSERT INTO search_dict (team_id, search_terms) VALUES (?, ?) "
                    "ON CONFLICT(team_id) DO UPDATE SET search_terms = excluded.search_terms",
                    (team_id, json.dumps(terms))
                )
                conn.commit()
        except Exception:
            pass  # Non-critical


    async def resolve_with_cascade(
        self,
        fs_fix: Dict,
        fb_matches: List[Dict],
        conn: sqlite3.Connection,
    ) -> Tuple[Optional[Dict], float, str]:
        """
        2-stage cascade:
          1. search_dict — exact alias lookup (sufficient once league is matched)
          2. llm — Gemini fallback for genuinely ambiguous team names

        Returns: (best_match_dict, score, method_str)
        """
        if not fb_matches:
            return None, 0.0, 'failed'

        home = (fs_fix.get('home_team_name') or fs_fix.get('home_team') or '').strip()
        away = (fs_fix.get('away_team_name') or fs_fix.get('away_team') or '').strip()
        home_id = fs_fix.get('home_team_id') or fs_fix.get('home_id')
        away_id = fs_fix.get('away_team_id') or fs_fix.get('away_id')

        if not home or not away:
            return None, 0.0, 'failed'

        # ── Stage 1: search_dict ─────────────────────────────────────────────
        home_terms = self._get_search_terms(conn, home_id)
        away_terms = self._get_search_terms(conn, away_id)
        # Include the raw FS name as a candidate alias
        h_aliases = [home.lower()] + [t.lower() for t in home_terms]
        a_aliases = [away.lower()] + [t.lower() for t in away_terms]

        for m in fb_matches:
            fb_h = self._get_name(m, 'home').lower()
            fb_a = self._get_name(m, 'away').lower()
            # Bidirectional: fb name IS an alias, OR fb name contains an alias, OR alias contains fb name
            h_match = fb_h in h_aliases or any(fb_h in alias for alias in h_aliases) or any(alias in fb_h for alias in h_aliases if len(alias) >= 4)
            a_match = fb_a in a_aliases or any(fb_a in alias for alias in a_aliases) or any(alias in fb_a for alias in a_aliases if len(alias) >= 4)
            if h_match and a_match:
                # Auto-learn fb name as alias for future sessions
                self._auto_learn(conn, home_id, self._get_name(m, 'home'))
                self._auto_learn(conn, away_id, self._get_name(m, 'away'))
                return {**m, 'matched': True}, 0.98, 'search_terms'

        # ── Stage 2: Gemini LLM ──────────────────────────────────────────────
        llm_match, llm_score = await self._llm_resolve(
            f"{home} vs {away}", fb_matches
        )
        if llm_match:
            # Auto-learn the LLM-resolved alias so next session uses search_dict
            self._auto_learn(conn, home_id, self._get_name(llm_match, 'home'))
            self._auto_learn(conn, away_id, self._get_name(llm_match, 'away'))
            return {**llm_match, 'matched': True}, llm_score, 'llm'

        return None, 0.0, 'failed'


    async def _llm_resolve(
        self,
        fs_name: str,
        fb_matches: List[Dict],
    ) -> Tuple[Optional[Dict], float]:
        """Gemini LLM fallback for genuinely ambiguous team name matches.
        Each model's error is isolated — one model's failure never kills others.
        Grok removed: retired model, not cost-effective (RULEBOOK §1 first principles).
        """
        if not HAS_GEMINI:
            return None, 0.0

        candidates = []
        for i, m in enumerate(fb_matches[:8]):
            fb_home = self._get_name(m, 'home')
            fb_away = self._get_name(m, 'away')
            candidates.append(f"{i}: {fb_home} vs {fb_away}")

        if not candidates:
            return None, 0.0

        prompt_text = (
            f"Match to find: '{fs_name}'\n"
            f"Candidates (index: home vs away):\n"
            + '\n'.join(candidates)
            + '\n\nReply with the index number ONLY of the best match, or -1 if none fits.'
        )

        # Gemini only — each model isolated, one 400/quota never kills others
        model_chain = ['gemini-2.0-flash-lite', 'gemini-1.5-flash-8b', 'gemini-2.0-flash']
        api_key = os.environ.get('GEMINI_API_KEY', '')
        if not api_key:
            return None, 0.0

        for model_name in model_chain:
            if model_name in _session_dead_models:
                continue
            try:
                client = genai.Client(api_key=api_key)
                response = client.models.generate_content(
                    model=model_name,
                    contents=prompt_text,
                    config=types.GenerateContentConfig(
                        temperature=0.0,
                        max_output_tokens=10,
                    )
                )
                answer = (response.text or '').strip()
                try:
                    idx = int(answer)
                    if 0 <= idx < len(fb_matches):
                        return fb_matches[idx], 0.80
                except ValueError:
                    pass
                # Got a parseable response but invalid index — this model works, stop
                break
            except Exception as e:
                err_str = str(e).lower()
                if 'quota' in err_str or '429' in err_str:
                    # Rate-limited — mark dead for entire session, try next
                    _session_dead_models.add(model_name)
                else:
                    # Transient error (400, 500, timeout) — skip for THIS fixture only,
                    # NOT permanent. Do NOT add to dead models — let next fixture retry.
                    break

        return None, 0.0
