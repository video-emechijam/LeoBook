import pandas as pd
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

class MetadataLinker:
    """
    Utility to link uploaded assets (public URLs) to the database by updating local CSVs.
    These CSVs are then synchronized to Supabase by the SyncManager.
    """

    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.teams_csv_path = project_root / "Data" / "Store" / "teams.csv"
        self.leagues_csv_path = project_root / "Data" / "Store" / "region_league.csv"

        # Load CSVs lazily
        self._teams_df: Optional[pd.DataFrame] = None
        self._leagues_df: Optional[pd.DataFrame] = None

        # Mapping overrides for scraper slugs to database league_ids
        self.MAPPING_OVERRIDES = {
            "english-premier-league": "OEEq9Yvp",
            "germany-bundesliga": "8UYeqfiD",
            "france-ligue-1": "j9QeTLPP",
            "netherlands-eredivisie": "dWKtjvdd",
            "italy-serie-a": "6PWwAsA7",
            "uefa-conference-league": "EUROPE_CONFERENCE_LEAGUE",
            "uefa-europa-league": "EUROPE_EUROPA_LEAGUE",
        }

    def _load_teams(self):
        if self._teams_df is None and self.teams_csv_path.exists():
            self._teams_df = pd.read_csv(self.teams_csv_path)
            # Ensure league_ids is handled as string for searching
            self._teams_df['league_ids'] = self._teams_df['league_ids'].fillna('').astype(str)

    def _load_leagues(self):
        if self._leagues_df is None and self.leagues_csv_path.exists():
            self._leagues_df = pd.read_csv(self.leagues_csv_path)

    def save(self):
        """Save updated dataframes back to CSV files."""
        if self._teams_df is not None:
            self._teams_df.to_csv(self.teams_csv_path, index=False)
            logger.info(f"📁 Updated {self.teams_csv_path}")
        if self._leagues_df is not None:
            self._leagues_df.to_csv(self.leagues_csv_path, index=False)
            logger.info(f"📁 Updated {self.leagues_csv_path}")

    def update_league_logo(self, league_slug: str, public_url: str) -> bool:
        """
        Updates region_league.csv with the public URL for a league.
        Maps slug (e.g. 'english-premier-league') to league_id (e.g. 'ENGLAND_PREMIER_LEAGUE').
        """
        self._load_leagues()
        if self._leagues_df is None:
            return False

        # Use override if available, otherwise normalize
        league_id = self.MAPPING_OVERRIDES.get(league_slug)
        if not league_id:
            league_id = league_slug.upper().replace("-", "_")
        
        # Check for direct match first
        match = self._leagues_df[self._leagues_df['league_id'] == league_id]
        
        # If no match, try fuzzy match on league name or search_terms
        if match.empty:
            if 'search_terms' in self._leagues_df.columns:
                match = self._leagues_df[self._leagues_df['search_terms'].str.contains(f"'{league_slug}'", case=False, na=False)]

        if match.empty:
            logger.warning(f"⚠️ Could not find exact match for league_id '{league_id}'")
            return False

        idx = match.index[0]
        self._leagues_df.at[idx, 'league_crest'] = public_url
        logger.info(f"✅ Linked league '{league_slug}' to {league_id}")
        return True

        idx = match.index[0]
        self._leagues_df.at[idx, 'league_crest'] = public_url
        logger.info(f"✅ Linked league '{league_slug}' to {league_id_guess}")
        return True

    def update_team_logo(self, team_name: str, league_slug: str, public_url: str) -> bool:
        """
        Updates teams.csv with the public URL for a team.
        Uses team_name and normalized league_id to filter.
        """
        self._load_teams()
        if self._teams_df is None:
            return False

        # Normalize team_name from scraper (strip suffixes)
        normalized_name = team_name.replace(".football-logos.cc", "").replace("-", " ").strip()

        league_id = self.MAPPING_OVERRIDES.get(league_slug)
        if not league_id:
            league_id = league_slug.upper().replace("-", "_")
        
        # Filter by league first
        league_teams = self._teams_df[self._teams_df['league_ids'].str.contains(league_id, na=False)]
        
        if league_teams.empty:
            logger.warning(f"⚠️ League '{league_id}' not found in teams.csv league_ids")
            search_df = self._teams_df
        else:
            search_df = league_teams

        # Match by normalized name (case insensitive)
        match = search_df[search_df['team_name'].str.lower() == normalized_name.lower()]
        
        if match.empty:
            # Try search_terms if available
            if 'search_terms' in search_df.columns:
                match = search_df[search_df['search_terms'].str.contains(f"'{normalized_name.lower()}'", case=False, na=False)]

        if match.empty:
            logger.warning(f"❌ Could not find team '{normalized_name}' in league '{league_id}'")
            return False

        idx = match.index[0]
        self._teams_df.at[idx, 'team_crest'] = public_url
        logger.info(f"✅ Linked team '{normalized_name}' in {league_id}")
        return True

    def update_region_flag(self, region_name: str, public_url: str) -> bool:
        """
        Updates region_league.csv with the public URL for a region's flag.
        Matches by the 'region' column (case-insensitive).
        """
        self._load_leagues()
        if self._leagues_df is None:
            return False

        # Match by region name (case insensitive)
        mask = self._leagues_df['region'].str.lower() == region_name.lower()
        if not mask.any():
            logger.warning(f"⚠️ Could not find region '{region_name}' in region_league.csv")
            return False

        self._leagues_df.loc[mask, 'region_flag'] = public_url
        logger.info(f"✅ Linked flag for region '{region_name}'")
        return True

if __name__ == "__main__":
    # Test block
    logging.basicConfig(level=logging.INFO)
    root = Path(__file__).parent.parent.parent
    linker = MetadataLinker(root)
    # linker.update_team_logo("Arsenal", "english-premier-league", "https://example.com/arsenal.png")
    # linker.save()
