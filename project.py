"""
Olympic Data Cleaning and Paris 2024 Integration Pipeline

This script implements the complete workflow required for Milestone 1 and
Milestone 2 of the Olympic Data Processing Project. It loads the historical
Olympics datasets, cleans and standardizes all fields (dates, names, countries),
integrates Paris 2024 data, and generates new derived outputs such as athlete
ages and medal tallies.

Major Responsibilities
----------------------
1. **Core Dataset Cleaning**
   - Normalize birthdates, games dates, countries/NOCs, and athlete bios.
   - Enforce consistent formats for all CSV fields across historical files.
   - Resolve malformed, partial, or inconsistent data entries.

2. **Paris 2024 Data Integration**
   - Match Paris athletes to existing bios using normalized names + NOC.
   - Create new athletes when needed and enrich missing biographical fields.
   - Generate Paris event results (individual + team) and attach medals.
   - Ensure all new rows include the correct edition_id for Paris 2024.

3. **Derived Data Generation**
   - Compute athlete ages for each event using standardized date ranges.
   - Produce a unified medal tally summarizing medals by edition and NOC.

4. **Output Artifact Creation**
   The script writes all final deliverables required for Milestone 2:
     - new_athlete_bio.csv
     - new_athlete_event_results.csv
     - new_countries.csv
     - new_games.csv
     - new_age.csv
     - new_medal_tally.csv

Design Notes
------------
- All processing is deterministic and idempotent: running the script twice on
  the same input produces identical output files.
- Helper functions are intentionally modular to support testing and reuse.
- The `main()` function serves as the orchestration layer coordinating the
  complete end-to-end pipeline.

This module is the central execution point for the project and contains no
side effects beyond reading input CSV files and writing cleaned output files.
"""

import csv
import re
import ast
import time
from typing import List, Dict, Tuple, Set, Optional

def read_csv_file(file_name: str) -> List[List[str]]:
    """
    Read a CSV file into a list of rows, where each row is a list of string values.

    Parameters
    ----------
    file_name : str
        The path or filename of the CSV file to read. The function expects a
        UTF-8 or UTF-8-SIG encoded file. If the file does not exist, a warning 
        is printed and an empty list is returned.

    Returns
    -------
    List[List[str]]
        A list-of-lists representing the CSV contents. Each inner list corresponds
        to one row in the file. If the file cannot be found, an empty list is returned.

    Notes
    -----
    - This function does not perform type conversion; all values are returned as strings.
    - Leading byte order marks (BOM) are handled via UTF-8-SIG decoding.
    - Intended as a simple CSV loader for all project datasets in Milestone 1 and 2.
    """
    data_set: List[List[str]] = []
    try:
        with open(file_name, mode="r", encoding="utf-8-sig") as file:
            csv_reader = csv.reader(file)
            for row in csv_reader:
                data_set.append(row)
    except FileNotFoundError:
        print(f"Warning: File {file_name} not found")
    return data_set

def write_csv_file(file_name: str, data_set: List[List[str]]) -> None:
    """
    Write a list-of-lists to a CSV file.

    Parameters
    ----------
    file_name : str
        The output path or filename where the CSV will be written. Existing files
        with the same name will be overwritten.

    data_set : List[List[str]]
        A list of rows to write, where each row is a list of string values. The
        structure must match the expected final CSV format for Milestone outputs
        (e.g., new_birthdates.csv, new_games.csv, new_age.csv).

    Returns
    -------
    None
        This function does not return a value; it writes data directly to disk.

    Notes
    -----
    - All rows are written exactly as provided; this function does not sanitize,
      validate, or transform data.
    - UTF-8 encoding is used for full compatibility with international characters.
    - Ensures proper newline handling across operating systems (via newline="").
    """
    with open(file_name, mode="w", newline="", encoding="utf-8") as file:
        csv_writer = csv.writer(file)
        csv_writer.writerows(data_set)


# ==================== DATE CLEANING FOR BORN COLUMN By Navish and Minhaz ====================

def clean_birth_date_enhanced(date_str: str) -> str:
    """
    Normalize inconsistent birthdate formats into a standard 'dd-Mon-yyyy' format.

    This function handles all date irregularities observed in the Olympic athlete
    dataset. It interprets multiple input formats, resolves ambiguous two-digit
    years, and extracts years embedded in free text. The output is always
    normalized to 'dd-Mon-yyyy', or an empty string if the date is invalid or
    cannot be interpreted.

    Accepted Input Formats
    ----------------------
    - 'dd-Mon-yy'       → e.g., '11-Aug-41'
    - 'dd-Mon-yyyy'     → e.g., '02-Feb-1997'
    - 'dd Month yyyy'   → e.g., '25 January 1884'
    - Year-only formats → '1884', '(1884)', 'circa 1884', 'c. 1884', etc.

    Two-Digit Year Rules
    --------------------
    - 00–07 → interpreted as 2000–2007  
    - 08–99 → interpreted as 1908–1999  
      (Based on project specification and typical Olympic-era data distribution.)

    Parameters
    ----------
    date_str : str
        The raw birthdate string from the dataset. May contain textual noise,
        parentheses, partial dates, or be empty/whitespace.

    Returns
    -------
    str
        A cleaned birthdate in the format 'dd-Mon-yyyy'.
        Returns an empty string "" if:
        - The input is missing or blank.
        - The date format is unrecognized.
        - Extracted components are invalid (e.g., invalid month or day).

    Notes
    -----
    - Month names (full or abbreviated) are accepted and normalized to
      three-letter abbreviations.
    - For year-only inputs, day defaults to '01-Jan'.
    - This function is designed specifically for the known irregularities in the
      Olympic data, not general-purpose date parsing.
    """

    if not date_str or not date_str.strip():
        return ""

    s = date_str.strip()

    # NEW: handle ISO-style dates from Paris files: yyyy-mm-dd
    iso_match = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", date_str)
    
    if iso_match:
        year = int(iso_match.group(1))
        month = int(iso_match.group(2))
        day = int(iso_match.group(3))

        # basic sanity checks
        if not (1800 <= year <= 2025 and 1 <= month <= 12 and 1 <= day <= 31):
            return ""

        month_abbrs = [
            "Jan", "Feb", "Mar", "Apr", "May", "Jun",
            "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
        ]
        month_abbr = month_abbrs[month - 1]

        # convert to the standard format used everywhere else
        return f"{day:02d}-{month_abbr}-{year}"
    
    # --- Month helpers (only what we need) ---
    def month_abbrev_from_any(name: str) -> Optional[str]:
        """
        Convert a month name or abbreviation (any casing) into a standardized
        three-letter month abbreviation (e.g., 'January' → 'Jan'). Returns None
        if the input does not match a known month.
        """
        name = name.strip().lower()
        month_map = {
            "january": "Jan", "february": "Feb", "march": "Mar", "april": "Apr",
            "may": "May", "june": "Jun", "july": "Jul", "august": "Aug",
            "september": "Sep", "october": "Oct", "november": "Nov", "december": "Dec",
            "jan": "Jan", "feb": "Feb", "mar": "Mar", "apr": "Apr",
            "jun": "Jun", "jul": "Jul", "aug": "Aug", "sep": "Sep",
            "oct": "Oct", "nov": "Nov", "dec": "Dec",
        }
        return month_map.get(name)

    # -------------------------------------------------
    # 1) dd-Mon-yy or dd-Mon-yyyy  (e.g. 11-Aug-41)
    # -------------------------------------------------
    m = re.match(r"^(\d{1,2})-([A-Za-z]{3})-(\d{2}|\d{4})$", s)
    if m:
        day = int(m.group(1))
        month = month_abbrev_from_any(m.group(2))
        year_str = m.group(3)

        if not month or not (1 <= day <= 31):
            return ""

        if len(year_str) == 2:
            y2 = int(year_str)
            # Your rule:
            #   00–07 -> 2000–2007
            #   08–99 -> 1900–1999
            if y2 < 8:
                year = 2000 + y2
            else:
                year = 1900 + y2
        else:
            year = int(year_str)

        return f"{day:02d}-{month}-{year}"

    # -------------------------------------------------
    # 2) dd Month yyyy  (e.g. 25 January 1884)
    # -------------------------------------------------
    m = re.match(r"^(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$", s)
    if m:
        day = int(m.group(1))
        month = month_abbrev_from_any(m.group(2))
        year = int(m.group(3))

        if month and 1 <= day <= 31:
            return f"{day:02d}-{month}-{year}"

    # -------------------------------------------------
    # 3) Year only OR year inside text
    #    (1884), circa 1884, c. 1884, etc.
    # -------------------------------------------------
    m = re.search(r"(\d{4})", s)
    if m:
        year = int(m.group(1))
        # No lower bound: allow years before 1880 as requested
        return f"01-Jan-{year}"

    # If nothing matches, treat as missing
    return ""

# ==================== DATE CLEANING FOR OLYMPIC GAMES By Minhaz and Navish  ====================

def clean_single_games_date_enhanced(date_str: str, year: int) -> str:
    """
    Normalize a single Olympic Games date into the format 'dd-Mon-yyyy', forcing
    the supplied `year` argument even when the input string contains a different
    year value.

    This function is used for Games datasets where the edition year is known and
    trusted, but date strings may contain misleading or incorrect years (e.g.,
    '23 July 2021' for the Tokyo 2020 Olympics). Only the day and month are
    extracted from the input; the final year is always overridden by the function
    argument.

    Accepted Input Patterns
    -----------------------
    - '6 April'              → day + month  
    - '23 July 2021'         → day + month + ignored year  
    - Supports both full month names and common abbreviations.

    Parameters
    ----------
    date_str : str
        The raw date text from the Games CSV. May contain month names in various
        forms, optional trailing years, or inconsistent formatting.

    year : int
        The corrected Olympic edition year that must be enforced in the cleaned
        output, even if the original date_str includes a conflicting year.

    Returns
    -------
    str
        A cleaned date formatted as 'dd-Mon-yyyy'.
        Returns an empty string "" if:
        - The input is empty or whitespace.
        - The format does not match one of the expected patterns.
        - Day or month components cannot be interpreted.

    Notes
    -----
    - Month names are normalized to three-letter English abbreviations.
    - The function intentionally ignores any explicit year found in `date_str`.
    - Only single-day dates are processed here; multi-range dates are handled by
      `clean_games_date_enhanced`.
    """

    if not date_str:
        return ""
    
    s = date_str.strip()
    if not s:
        return ""

    month_map = {
        'january': 'Jan', 'february': 'Feb', 'march': 'Mar', 'april': 'Apr',
        'may': 'May', 'june': 'Jun', 'july': 'Jul', 'august': 'Aug',
        'september': 'Sep', 'october': 'Oct', 'november': 'Nov', 'december': 'Dec',
        'jan': 'Jan', 'feb': 'Feb', 'mar': 'Mar', 'apr': 'Apr',
        'jun': 'Jun', 'jul': 'Jul', 'aug': 'Aug', 'sep': 'Sep',
        'oct': 'Oct', 'nov': 'Nov', 'dec': 'Dec'
    }

    parts = s.split()
    
    # Case: "6 April"
    if len(parts) == 2:
        d, m = parts
        if d.isdigit():
            abbr = month_map.get(m.lower())
            if abbr:
                return f"{int(d):02d}-{abbr}-{year}"

    # Case: "23 July 2021" -> We must IGNORE 2021 and use `year` (2020)
    elif len(parts) == 3:
        d, m, y_str = parts
        if d.isdigit() and y_str.isdigit():
            abbr = month_map.get(m.lower())
            if abbr:
                # CHANGED: We use {year} here, not {y_str}
                return f"{int(d):02d}-{abbr}-{year}"
                
    return ""

def clean_games_date_enhanced(date_str: str, year: int) -> str:
    """
    Clean and normalize Olympic Games date ranges into the format
    'dd-Mon-yyyy to dd-Mon-yyyy', ensuring that both the start and end dates use
    a consistent and correct year.

    This function handles the wide variety of Games date formats found in the
    dataset, especially ranges where the textual dates may contain misleading or
    incorrect years (e.g., “21 July – 8 August 2021” for the 2020 Olympics).
    The end-date year is treated as authoritative, and the start-date year is
    adjusted to match it.

    Supported Input Examples
    ------------------------
    - "21 July – 8 August 2021"
    - "6 – 13 April"
    - "3 August 1980 – 10 August 1980"
    - Single dates such as "23 July 2021" (passed to clean_single_games_date_enhanced)

    Behaviour
    ---------
    1. Splits the string on " – " to detect date ranges.
    2. Cleans the end date first using `clean_single_games_date_enhanced`.
       - If the end date contains an incorrect year, the supplied `year` parameter
         is enforced.
    3. Extracts the year from the cleaned end date.
       - This becomes the authoritative `actual_year`.
    4. Cleans the start date using the extracted `actual_year`.
       - Handles patterns like "6 – 13 April" where the start part may contain
         only the day number.
    5. Returns a normalized "start to end" string.

    Parameters
    ----------
    date_str : str
        The raw Games date text from the dataset. May contain irregular spacing,
        date ranges, missing years, or mismatched years.

    year : int
        The expected Olympic edition year. Used when the input date is missing a
        year or contains an incorrect one, but may be overridden by the correct
        year extracted from the end date.

    Returns
    -------
    str
        - For ranges: "dd-Mon-yyyy to dd-Mon-yyyy".
        - For single dates: "dd-Mon-yyyy".
        - Returns an empty string "" if the input is missing or cannot be parsed.

    Notes
    -----
    - This function resolves cross-year issues (e.g., where Games run across two
      months or where the dataset contains inconsistent year labels).
    - Day-only start dates are interpreted based on the month/year of the end date.
    - Multi-day date ranges are normalized using `clean_single_games_date_enhanced`
      for both start and end components.
    """
    
    if not date_str or date_str in {"", "—", "--", "–"}:
        return ""
    
    s = date_str.strip()
    
    # Split on " – "
    parts = s.split(" – ", 1)
    
    if len(parts) == 2:
        start_raw = parts[0].strip()
        end_raw = parts[1].strip()
        
        # 1. Clean the end date first
        # If end_raw is "8 August 2021", end_date becomes "08-Aug-2021"
        end_date = clean_single_games_date_enhanced(end_raw, year)
        
        if not end_date:
            return ""

        # 2. Extract the actual year from the cleaned end date
        # end_date format is always "DD-Mon-YYYY", so last 4 chars are the year.
        try:
            actual_year = int(end_date[-4:])
        except (ValueError, IndexError):
            actual_year = year

        # 3. Clean Start Date using the ACTUAL year, not the default year
        start_date = ""
        
        if start_raw.isdigit():
            # Handle "6 – 13 April" -> "06-Apr-YYYY"
            start_date = f"{int(start_raw):02d}{end_date[2:]}"
        else:
            # Handle "21 July – 8 August 2021"
            # We pass 'actual_year' (2021) here instead of 'year' (2020)
            start_date = clean_single_games_date_enhanced(start_raw, actual_year)
            
        if start_date:
            return f"{start_date} to {end_date}"
            
    # Fallback: Treat as single date
    return clean_single_games_date_enhanced(s, year)

# ==================== CLEAN ATHLETE BIO By Navish ====================

def clean_athlete_data(athlete_rows: List[List[str]]) -> Tuple[List[List[str]], Dict[str, str], Dict[str, str]]:
    """
    Clean the Olympic athlete biography dataset and extract standardized birthdates,
    while also building a mapping used for duplicate detection across datasets.

    This function:
    1. Validates the header and required columns.
    2. Normalizes missing row lengths to match the header.
    3. Cleans the "born" column using `clean_birth_date_enhanced`.
    4. Builds:
       - A dictionary of athlete_id → cleaned birthdate
       - A dictionary of (name + NOC) → athlete_id, used for identifying duplicates
         when merging Paris 2024 data.

    Parameters
    ----------
    athlete_rows : List[List[str]]
        Raw CSV content for olympic_athlete_bio.csv, where:
        - Row 0 is the header.
        - Each subsequent row represents an athlete.
        - Some rows may have missing fields or inconsistent formatting.

    Returns
    -------
    Tuple[
        List[List[str]],
        Dict[str, str],
        Dict[str, str]
    ]
        A tuple containing:
        
        1. **cleaned_rows** : List[List[str]]
           The athlete dataset with birthdates cleaned and row lengths normalized.

        2. **birth_dates** : Dict[str, str]
           Mapping of athlete_id → cleaned birthdate ('dd-Mon-yyyy').
           Only includes athletes with valid interpretable dates.

        3. **athlete_name_noc_map** : Dict[str, str]
           Mapping of "name_noc" (lowercased name + uppercase NOC) → athlete_id.
           Used in Paris data integration to prevent adding duplicate athletes.

    Notes
    -----
    - Rows shorter than the header are padded with empty strings to maintain structure.
    - All name matching for duplicate detection is done in lowercase (name) and
      uppercase (NOC) to improve consistency.
    - If required header fields ('athlete_id', 'born', 'name', 'country_noc') are 
      missing, the function returns the dataset unchanged with empty dictionaries.
    - This function does not modify or validate other fields such as gender or height.
    """

    if not athlete_rows or len(athlete_rows) < 2:
        return athlete_rows, {}
    
    header = athlete_rows[0]
    try:
        id_idx = header.index("athlete_id")
        born_idx = header.index("born")
        name_idx = header.index("name")
        noc_idx = header.index("country_noc")
    except ValueError:
        return athlete_rows, {}
    
    cleaned = [header]
    birth_dates = {}
    athlete_name_noc_map = {}  # For duplicate checking
    
    for row in athlete_rows[1:]:
        if len(row) < len(header):
            row = row + [""] * (len(header) - len(row))
        
        athlete_id = row[id_idx].strip()
        name = row[name_idx].strip().lower()
        noc = row[noc_idx].strip().upper()
        
        # Store for duplicate checking
        key = f"{name}_{noc}"
        athlete_name_noc_map[key] = athlete_id
        
        # Clean birth date
        if born_idx < len(row):
            original = row[born_idx]
            cleaned_date = clean_birth_date_enhanced(original)
            row[born_idx] = cleaned_date
            
            if cleaned_date:
                birth_dates[athlete_id] = cleaned_date
        
        cleaned.append(row)
    
    return cleaned, birth_dates, athlete_name_noc_map

# ==================== CLEAN GAMES DATA By Navish ====================

def clean_games_data(games_rows: List[List[str]]) -> Tuple[List[List[str]], str]:
    """
    Clean and standardize Olympic Games date fields (start_date, end_date,
    competition_date) while also identifying the edition_id corresponding to the
    Paris 2024 Olympic Games.

    This function:
    1. Validates the header and required columns.
    2. Pads rows to maintain consistent length with the header.
    3. Cleans all date-related fields using:
       - clean_single_games_date_enhanced   (for single-day dates)
       - clean_games_date_enhanced          (for multi-day ranges)
    4. Special-cases Paris 2024 dates due to known irregularities in the source
       data, forcing them to:
         - Start Date:       "26-Jul-2024"
         - End Date:         "11-Aug-2024"
         - Competition Date: "24-Jul-2024 to 11-Aug-2024"
    5. Returns both the cleaned dataset and the Paris 2024 edition_id so it can
       be referenced later during Paris event integration.

    Parameters
    ----------
    games_rows : List[List[str]]
        The rows from olympics_games.csv. Row 0 must contain the header, and
        subsequent rows represent individual Olympic editions. Some rows may
        contain missing values, inconsistent years, or malformed date strings.

    Returns
    -------
    Tuple[List[List[str]], str]
        A tuple containing:

        1. **cleaned_rows** : List[List[str]]
            The original dataset with all date fields normalized. All rows are
            guaranteed to match header length via padding, and all dates conform
            to 'dd-Mon-yyyy' or standardized ranges.

        2. **paris_edition_id** : str
            The edition_id for the Paris 2024 Olympics, extracted by detecting:
                - year == 2024, AND
                - city contains 'paris' OR edition text contains '2024'
            Returns "" if Paris 2024 is not found.

    Notes
    -----
    - This function assumes that the Paris 2024 row should override all source
      date values due to known inconsistencies.
    - Date parsing uses the `year` column as the authoritative indicator of
      the Olympic edition, except for Paris (handled separately).
    - competition_date may be a range such as "4 – 22 February". The enhanced
      cleaner automatically processes this into a canonical format.
    - If required columns ('edition', 'edition_id', 'year', 'city', 'start_date',
      'end_date', 'competition_date') are missing, the function returns the input 
      unchanged with paris_edition_id = "".
    """

    if not games_rows or len(games_rows) < 2:
        return games_rows, ""
    
    header = games_rows[0]
    try:
        # Map indices once for speed
        col_map = {name: i for i, name in enumerate(header)}
        idx_ed = col_map["edition"]
        idx_id = col_map["edition_id"]
        idx_yr = col_map["year"]
        idx_city = col_map["city"]
        idx_start = col_map["start_date"]
        idx_end = col_map["end_date"]
        idx_comp = col_map["competition_date"]
    except KeyError:
        return games_rows, ""
    
    cleaned = [header]
    paris_edition_id = ""
    
    # Padding cache to avoid recreating list every loop
    header_len = len(header)
    empty_pad = [""] * header_len

    for row in games_rows[1:]:
        # Fast padding
        row_len = len(row)
        if row_len < header_len:
            row.extend(empty_pad[:header_len - row_len])

        # Parse Year
        try:
            year = int(row[idx_yr])
        except (ValueError, TypeError):
            year = None

        # Check Paris 2024 (Lazy check: only lower() if year is 2024)
        is_paris_2024 = False
        if year == 2024:
            if "paris" in row[idx_city].lower() or "2024" in row[idx_ed]:
                is_paris_2024 = True
                if not paris_edition_id:
                    paris_edition_id = row[idx_id]

        if is_paris_2024:
            row[idx_start] = "26-Jul-2024"
            row[idx_end] = "11-Aug-2024"
            row[idx_comp] = "24-Jul-2024 to 11-Aug-2024"
        elif year:
            # Standard Cleaning
            # Note: start/end are single dates, competition is a range
            if row[idx_start]:
                row[idx_start] = clean_single_games_date_enhanced(row[idx_start], year)
            
            if row[idx_end]:
                row[idx_end] = clean_single_games_date_enhanced(row[idx_end], year)
                
            if row[idx_comp]:
                # This now handles "4 – 22 February" automatically
                row[idx_comp] = clean_games_date_enhanced(row[idx_comp], year)

        cleaned.append(row)

    return cleaned, paris_edition_id

# ==================== CLEAN COUNTRIES By Navish ====================

def clean_countries(country_rows: List[List[str]], paris_nocs: List[List[str]]) -> List[List[str]]:
    """
    Clean and consolidate country/NOC information from the main dataset and the
    Paris 2024 NOC file, producing a unified, alphabetically sorted list of
    country codes and names.

    This function:
    1. Extracts existing country NOC mappings from olympics_country.csv.
    2. Normalizes NOC codes to uppercase and trims whitespace from fields.
    3. Incorporates additional NOCs from the Paris 2024 file (paris_nocs.csv),
       adding only those not already present in the main dataset.
    4. Returns a clean, deduplicated, and alphabetically sorted list based on
       the country name field.

    Parameters
    ----------
    country_rows : List[List[str]]
        The rows from olympics_country.csv.  
        - Row 0 must contain at least 'noc' and 'country' columns.  
        - Later rows may contain missing or extra columns, which are safely ignored.

    paris_nocs : List[List[str]]
        The rows from the Paris 2024 NOC code file.  
        Must contain 'code' and 'country' columns; otherwise, no Paris entries
        are added. Empty or malformed files are safely ignored.

    Returns
    -------
    List[List[str]]
        A cleaned list-of-lists where:
        - Row 0 is the original header ['noc', 'country'].
        - Each subsequent row contains a unique (NOC, country) pair.
        - All NOCs are uppercase.
        - Rows are sorted alphabetically by country name.

    Notes
    -----
    - Existing dataset values always take priority over Paris 2024 entries.
    - Missing header fields result in returning the original dataset unchanged.
    - Sorting ensures stable output for Milestone scoring and CSV comparison.
    - The function outputs only the two required columns; any additional columns
      in the input files are ignored.
    """

    if not country_rows:
        return [["noc", "country"]]
    
    header = country_rows[0]
    try:
        noc_idx = header.index("noc")
        country_idx = header.index("country")
    except ValueError:
        return country_rows
    
    country_map = {}
    
    # Add existing countries
    for row in country_rows[1:]:
        if len(row) > max(noc_idx, country_idx):
            noc = row[noc_idx].strip().upper()
            country = row[country_idx].strip()
            if noc:
                country_map[noc] = country
    
    # Add Paris NOCs
    if paris_nocs and len(paris_nocs) > 1:
        p_header = paris_nocs[0]
        try:
            p_code_idx = p_header.index("code")
            p_country_idx = p_header.index("country")
        except ValueError:
            p_code_idx = p_country_idx = -1
        
        if p_code_idx >= 0 and p_country_idx >= 0:
            for row in paris_nocs[1:]:
                if len(row) > max(p_code_idx, p_country_idx):
                    noc = row[p_code_idx].strip().upper()
                    country = row[p_country_idx].strip()
                    if noc and noc not in country_map:
                        country_map[noc] = country
    
    # Sort by country name
    sorted_items = sorted(country_map.items(), key=lambda x: x[1].lower())
    result = [header]
    for noc, country in sorted_items:
        result.append([noc, country])
    
    return result

# ==================== CALCULATE AGE by Minhaz ====================

def calculate_age(birth_date: str, event_date: str) -> str:
    """
    Compute an athlete's age for a given Olympic edition based on the official
    games start date, with a special adjustment when the athlete's birthday falls
    during the games window.

    This function supports both single-date and date-range formats for Olympic
    events and applies project-specific rules for determining an athlete's age at
    the time of competition. All inputs must already be normalized to the
    'dd-Mon-yyyy' format produced by the cleaning functions.

    Age Calculation Rules
    ---------------------
    1. **Primary rule**  
       Age is calculated as of the *start date* of the Olympic Games.

    2. **Birthday-during-games adjustment**  
       If the athlete's birthday (month/day) occurs **between the start and end
       of the games, inclusive**, the athlete is treated as if they have already
       celebrated that birthday before the games began.  
       → In this case, the calculated age is increased by **+1**.

    Supported Event Date Formats
    ----------------------------
    - 'dd-Mon-yyyy'
    - 'dd-Mon-yyyy to dd-Mon-yyyy'

    Parameters
    ----------
    birth_date : str
        Athlete birth date in the cleaned format 'dd-Mon-yyyy'. Must be valid and
        parsable; otherwise the function returns "".

    event_date : str
        The competition date(s) for the Olympic edition. May be a single start
        date or a start–end range. Must also be in cleaned format.

    Returns
    -------
    str
        The computed age as a string. Returns "" if:
        - Either input is missing or improperly formatted.
        - Dates cannot be parsed.
        - The resulting age is below 0 or above 120.
        - A computation error occurs.

    Notes
    -----
    - Month abbreviations must be three-letter English abbreviations
      (Jan, Feb, Mar, ...), matching the project's date-cleaning conventions.
    - When a date range is provided, only the start date affects the baseline age,
      but the end date is used to determine the birthday-during-games exception.
    - The function is intentionally defensive: any parsing failure or unreasonable
      age results in returning an empty string.
    """

    if not birth_date or not event_date:
        return ""

    birth_date = birth_date.strip()
    event_date = event_date.strip()

    # Month mapping
    month_map = {
        'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4,
        'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8,
        'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
    }

    def parse_dd_mon_yyyy(s: str):
        """
        Parse a date string in the strict 'dd-Mon-yyyy' format and return
        (year, month, day) as integers. Returns None if the string does not
        match the expected pattern or contains invalid day/month values.
        """
        m = re.match(r"^\s*(\d{1,2})-([A-Z][a-z]{2})-(\d{4})\s*$", s)
        if not m:
            return None
        day = int(m.group(1))
        mon_abbr = m.group(2)
        year = int(m.group(3))
        month = month_map.get(mon_abbr)
        if not month or not (1 <= day <= 31):
            return None
        return year, month, day

    try:
        # Parse birth date
        b = parse_dd_mon_yyyy(birth_date)
        if not b:
            return ""
        birth_year, birth_month, birth_day = b

        # Parse event date(s)
        if "to" in event_date:
            start_str, end_str = [part.strip() for part in event_date.split("to", 1)]
        else:
            start_str, end_str = event_date, ""

        start_parsed = parse_dd_mon_yyyy(start_str)
        if not start_parsed:
            return ""

        start_year, start_month, start_day = start_parsed

        end_parsed = parse_dd_mon_yyyy(end_str) if end_str else None
        if end_parsed:
            end_year, end_month, end_day = end_parsed
        else:
            end_year = end_month = end_day = None

        # --- Age as of the START of the games (normal rule) ---
        age = start_year - birth_year
        if (start_month, start_day) < (birth_month, birth_day):
            age -= 1

        # --- Special rule: birthday occurs DURING the games window ---
        # Represent the athlete's birthday in the games year
        # (we only care about month/day compared to the games dates)
        if end_parsed:
            # birthday "this games year"
            b_games_year = start_year
            # Compare tuples like (year, month, day)
            start_tuple = (start_year, start_month, start_day)
            end_tuple = (end_year, end_month, end_day)
            birthday_tuple = (b_games_year, birth_month, birth_day)

            if start_tuple <= birthday_tuple <= end_tuple:
                # Pretend the birthday already happened before the games began
                age += 1

        # Sanity check
        if age < 0 or age > 120:
            return ""

        return str(age)
    except Exception:
        return ""

# ==================== ADD AGE COLUMN by Minhaz ====================

def add_age_to_events(event_rows: List[List[str]],
                      birth_dates: Dict[str, str],
                      games_rows: List[List[str]]) -> List[List[str]]:
    """
    Append an 'age' column to the athlete event results by computing each athlete's
    age for the specific Olympic edition in which they competed.

    This function links three datasets:
      • event_rows  → athlete_id + edition_id  
      • birth_dates → athlete_id → birthdate (cleaned 'dd-Mon-yyyy')  
      • games_rows  → edition_id → competition/start/end dates  

    For each event row:
      1. Determine the event's date window based on edition_id:
         - Prefer 'competition_date' if present.
         - Otherwise build a range 'start_date to end_date'.
         - If only start_date exists, use that single date.
      2. Retrieve the athlete's cleaned birthdate.
      3. Compute age using calculate_age(), which applies the birthday-during-games
         adjustment when appropriate.
      4. Append the resulting age to the output dataset.

    Parameters
    ----------
    event_rows : List[List[str]]
        The rows of olympic_athlete_event_results.csv. Row 0 must contain
        'edition_id' and 'athlete_id'. Some rows may have missing fields and will
        be padded to match the header length.

    birth_dates : Dict[str, str]
        Mapping of athlete_id → cleaned birthdate ('dd-Mon-yyyy'), produced by
        clean_athlete_data(). Athletes without valid birthdates cannot have ages
        computed and will receive "".

    games_rows : List[List[str]]
        The rows of olympics_games.csv used to map each edition_id to a valid
        event date or date range. Missing or malformed date fields are handled
        gracefully.

    Returns
    -------
    List[List[str]]
        A new list-of-lists identical to event_rows but with an additional 'age'
        column appended. Each entry contains:
        - A string age value (e.g., "23"), or
        - "" if age cannot be determined.

    Notes
    -----
    - The function builds a complete edition_id → event_date mapping before
      processing events to avoid repeated lookups.
    - competition_date is always prioritized because it represents the true
      athletic competition period.
    - Age is always computed using cleaned dates; uncleaned or missing dates
      result in "".
    - Athletes with missing edition_id, missing athlete_id, or missing birthdate
      automatically receive an empty age entry.
    """

    if not event_rows or len(event_rows) < 2:
        return event_rows

    header = event_rows[0]
    try:
        edition_id_idx = header.index("edition_id")
        athlete_id_idx = header.index("athlete_id")
    except ValueError:
        return event_rows

    # --- Build edition_id -> event_date mapping ---
    edition_dates: Dict[str, str] = {}
    if games_rows and len(games_rows) > 1:
        g_header = games_rows[0]
        try:
            g_edition_id_idx = g_header.index("edition_id")
        except ValueError:
            g_edition_id_idx = -1

        # Optional indices – protect with try/except
        try:
            g_start_idx = g_header.index("start_date")
        except ValueError:
            g_start_idx = -1

        try:
            g_end_idx = g_header.index("end_date")
        except ValueError:
            g_end_idx = -1

        try:
            g_comp_idx = g_header.index("competition_date")
        except ValueError:
            g_comp_idx = -1

        if g_edition_id_idx >= 0:
            for row in games_rows[1:]:
                if len(row) <= g_edition_id_idx:
                    continue

                edition_id = row[g_edition_id_idx].strip()
                if not edition_id:
                    continue

                event_date = ""

                # 1) Prefer competition_date if present
                if 0 <= g_comp_idx < len(row):
                    comp = row[g_comp_idx].strip()
                    if comp:
                        event_date = comp

                # 2) Otherwise build from start_date / end_date
                if not event_date:
                    start = row[g_start_idx].strip() if 0 <= g_start_idx < len(row) else ""
                    end = row[g_end_idx].strip() if 0 <= g_end_idx < len(row) else ""

                    if start and end:
                        event_date = f"{start} to {end}"
                    elif start:
                        event_date = start
                    elif end:
                        event_date = end

                if event_date:
                    edition_dates[edition_id] = event_date

    # --- Add age column to event rows ---
    new_header = header + ["age"]
    result = [new_header]

    for row in event_rows[1:]:
        if len(row) < len(header):
            row = row + [""] * (len(header) - len(row))

        athlete_id = row[athlete_id_idx].strip()
        edition_id = row[edition_id_idx].strip()

        age = ""
        if athlete_id and edition_id:
            birth = birth_dates.get(athlete_id, "")
            event_date = edition_dates.get(edition_id, "")
            if birth and event_date:
                age = calculate_age(birth, event_date)

        result.append(row + [age])

    return result

# ==================== GENERATE MEDAL TALLY by Gurjeet ====================

def generate_medal_tally(event_rows: List[List[str]],
                         country_rows: List[List[str]],
                         games_rows: List[List[str]]) -> List[List[str]]:
    """
    Generate a country-level Olympic medal tally per edition, including the
    number of participating athletes and counts of gold, silver, bronze, and
    total medals.

    This function aggregates event-level results into a summary table with one
    row per (edition_id, NOC) combination. It joins three datasets:

      • event_rows   → edition_id, edition, country_noc, medal, athlete_id  
      • country_rows → noc → country name  
      • games_rows   → edition_id → edition (human-readable edition name)

    For each edition/NOC pair the function:
      1. Counts the number of **unique athletes** (by athlete_id).
      2. Counts medals by type (Gold, Silver, Bronze).
      3. Computes total_medals = gold + silver + bronze.
      4. Resolves the human-readable edition name and country name via lookups.

    Parameters
    ----------
    event_rows : List[List[str]]
        The rows from olympic_athlete_event_results.csv.  
        Must contain at least the columns: 'edition', 'edition_id',
        'country_noc', 'medal', and 'athlete_id'. Rows with missing key fields
        are skipped.

    country_rows : List[List[str]]
        The rows from olympics_country.csv (already cleaned/merged if using
        clean_countries). Used to map NOC → Country.  
        Must contain 'noc' and 'country' columns; otherwise, the NOC code is
        used as a fallback for the Country field.

    games_rows : List[List[str]]
        The rows from olympics_games.csv. Used to map edition_id → edition name.
        Must contain 'edition_id' and 'edition' columns; otherwise, the edition
        name from the event_rows is used as a fallback.

    Returns
    -------
    List[List[str]]
        A list-of-lists representing the new medal tally CSV with header:

            ["edition", "edition_id", "Country", "NOC",
             "number_of_athletes", "gold_medal_count",
             "silver_medal_count", "bronze_medal_count", "total_medals"]

        - One row per unique (edition_id, NOC) pair.
        - number_of_athletes counts distinct athlete_id values per pair.
        - Medal counts are integer values converted to strings for CSV output.
        - If event_rows is missing or lacks required columns, only the header
          row is returned.

    Notes
    -----
    - NOC codes are normalized to uppercase when building lookups.
    - country_rows and games_rows are optional but improve readability by
      providing proper country and edition names.
    - Sorting is by edition_id (numerically where possible) and then NOC, to
      ensure stable, predictable output for marking and comparison.
    - Only 'Gold', 'Silver', and 'Bronze' are counted; other medal values are
      ignored for tally purposes.
    """

    header = ["edition", "edition_id", "Country", "NOC",
              "number_of_athletes", "gold_medal_count",
              "silver_medal_count", "bronze_medal_count", "total_medals"]

    if not event_rows or len(event_rows) < 2:
        return [header]

    try:
        edition_idx = event_rows[0].index("edition")
        edition_id_idx = event_rows[0].index("edition_id")
        noc_idx = event_rows[0].index("country_noc")
        medal_idx = event_rows[0].index("medal")
        athlete_id_idx = event_rows[0].index("athlete_id")
    except ValueError:
        return [header]

    try:
        team_idx = event_rows[0].index("isTeamSport")
    except ValueError:
        team_idx = -1

    try:
        event_idx = event_rows[0].index("event")
    except ValueError:
        event_idx = -1

    noc_to_country: Dict[str, str] = {}
    if country_rows and len(country_rows) > 1:
        c_header = country_rows[0]
        try:
            c_noc_idx = c_header.index("noc")
            c_country_idx = c_header.index("country")
        except ValueError:
            c_noc_idx = c_country_idx = -1

        if c_noc_idx >= 0 and c_country_idx >= 0:
            for row in country_rows[1:]:
                if len(row) > c_country_idx:
                    noc = row[c_noc_idx].strip().upper()
                    country = row[c_country_idx].strip()
                    if noc:
                        noc_to_country[noc] = country

    edition_id_to_name: Dict[str, str] = {}
    if games_rows and len(games_rows) > 1:
        g_header = games_rows[0]
        try:
            g_edition_id_idx = g_header.index("edition_id")
            g_edition_idx = g_header.index("edition")
        except ValueError:
            g_edition_id_idx = g_edition_idx = -1

        if g_edition_id_idx >= 0 and g_edition_idx >= 0:
            for row in games_rows[1:]:
                if len(row) > g_edition_idx:
                    eid = row[g_edition_id_idx].strip()
                    ename = row[g_edition_idx].strip()
                    if eid:
                        edition_id_to_name[eid] = ename

    tally: Dict[Tuple[str, str], Dict[str, object]] = {}
    seen_paris_team_medals: Set[Tuple[str, str, str, str]] = set()

    for row in event_rows[1:]:
        if len(row) <= max(edition_id_idx, noc_idx, medal_idx, athlete_id_idx, edition_idx):
            continue

        edition = row[edition_idx].strip()
        edition_id = row[edition_id_idx].strip()
        noc = row[noc_idx].strip().upper()
        medal = row[medal_idx].strip()
        athlete_id = row[athlete_id_idx].strip()

        if not edition_id or not noc or not athlete_id:
            continue

        is_team = False
        if team_idx >= 0 and team_idx < len(row):
            is_team = row[team_idx].strip().lower() == "true"

        event_name = ""
        if event_idx >= 0 and event_idx < len(row):
            event_name = row[event_idx].strip()

        is_paris_2024 = "2024" in edition and "summer" in edition.lower()

        key = (edition_id, noc)
        if key not in tally:
            tally[key] = {
                "edition": edition,
                "athletes": set(),
                "gold": 0,
                "silver": 0,
                "bronze": 0,
            }

        tally[key]["athletes"].add(athlete_id)

        if not medal:
            continue

        if is_paris_2024 and is_team and event_name:
            team_key = (edition_id, noc, event_name, medal)
            if team_key in seen_paris_team_medals:
                continue
            seen_paris_team_medals.add(team_key)

        if medal == "Gold":
            tally[key]["gold"] += 1
        elif medal == "Silver":
            tally[key]["silver"] += 1
        elif medal == "Bronze":
            tally[key]["bronze"] += 1

    result: List[List[str]] = [header]

    def sort_key(item: Tuple[Tuple[str, str], Dict[str, object]]):
        (edition_id, noc), _ = item
        try:
            return (int(edition_id), noc)
        except ValueError:
            return (0, noc)

    for (edition_id, noc), data in sorted(tally.items(), key=sort_key):
        edition_name = edition_id_to_name.get(edition_id, data["edition"]) 
        country = noc_to_country.get(noc, noc)
        athletes_set: Set[str] = data["athletes"] 
        num_athletes = len(athletes_set)
        gold = int(data["gold"]) 
        silver = int(data["silver"])
        bronze = int(data["bronze"])
        total = gold + silver + bronze

        result.append([
            edition_name,
            edition_id,
            country,
            noc,
            str(num_athletes),
            str(gold),
            str(silver),
            str(bronze),
            str(total),
        ])

    return result

# ==================== PARIS INTEGRATION by Gurjeet and Minhaz  ====================
def format_athlete_name(name: str) -> str:
    """
    Normalize a raw name string into 'First Last' formatting by applying
    consistent casing rules. This helper ensures names from multiple data
    sources follow a unified style needed for duplicate detection and clean
    output files.

    Behavior
    --------
    - Converts the entire string to lowercase first.
    - Splits the name into words and capitalizes the first letter of each word.
    - Handles simple hyphenated name parts by capitalizing each segment
      (e.g., 'van-der' → 'Van-Der').
    - Removes leading/trailing whitespace and returns an empty string if the
      input is missing or blank.

    Parameters
    ----------
    name : str
        The raw name text (may include lowercase, uppercase, or mixed case,
        and optional hyphens).

    Returns
    -------
    str
        The normalized name in 'First Last' style. Returns "" if no valid name
        content is provided.

    Notes
    -----
    - This does not attempt to reorder names; it only fixes capitalization.
    - Used primarily by normalize_paris_name() to enforce uniform formatting
      after name matching or flipping logic.
    """

    name = (name or "").strip()
    if not name:
        return ""

    words = name.lower().split()
    formatted_words = []
    for w in words:
        # Handle hyphenated parts inside a word
        parts = w.split("-")
        parts = [p[:1].upper() + p[1:] if p else "" for p in parts]
        formatted_words.append("-".join(parts))
    return " ".join(formatted_words)

def normalize_paris_name(raw_name: str, alt_display: str = "") -> str:
    """
    Normalize athlete names from the Paris 2024 datasets so they match the
    naming format used in the main olympic_athlete_bio.csv file
    (i.e., 'First LAST').

    The Paris data often contains names in different formats, including:
      • A preferred TV/broadcast-style display name ('First LAST')
      • A reversed format such as 'LAST First'
      • Irregular capitalization patterns

    This function ensures that names from Paris data can be compared reliably
    against the cleaned main athlete dataset when checking for duplicates.

    Normalization Rules
    -------------------
    1. If `alt_display` is provided (e.g., a TV-friendly 'First LAST' format),
       **prefer it** immediately.
    2. If the raw name matches the common reversed Paris format:
           'LAST First'
       (detected as first token fully uppercase and second token lowercase),
       flip it to:
           'First LAST'
    3. Otherwise, return the cleaned raw name unchanged.

    Parameters
    ----------
    raw_name : str
        The original athlete name string from the Paris dataset. May be empty,
        improperly formatted, or in 'LAST First' order.

    alt_display : str, optional
        A cleaner, already-normalized name (e.g., from 'name_tv'). If provided,
        it is always preferred over `raw_name`.

    Returns
    -------
    str
        A standardized athlete name in 'First LAST' format, or "" if no valid
        name information is available.

    Notes
    -----
    - This function does not attempt multilingual or multi-part name handling.
      It focuses on the specific patterns observed in the Paris CSVs.
    - The normalization is intentionally conservative: only clear two-token
      reversals ('LAST First') are flipped.
    - Used extensively during Paris integration to identify whether a Paris
      athlete already exists in the main dataset (duplicate detection).
    """

    raw_name = (raw_name or "").strip()
    alt_display = (alt_display or "").strip()

    # 1) Prefer TV/display name if we have it (usually 'First LAST')
    if alt_display:
        return format_athlete_name(alt_display)

    if not raw_name:
        return ""

    parts = raw_name.split()
    # Typical Paris pattern: 'ALEKSANYAN Artur'
    if len(parts) == 2:
        first, second = parts
        if first.isupper() and not second.isupper():
            # LAST First -> First Last
            flipped = f"{second} {first}"
            return format_athlete_name(flipped)

    # Case B: 'LAST First Middle ...'
    if len(parts) >= 3:
        last = parts[0]
        rest = parts[1:]
        # If first token is all caps and the rest are not all caps,
        # it's very likely 'LAST First Middle'
        if last.isupper() and not all(p.isupper() for p in rest):
            flipped = " ".join(rest + [last])
            return format_athlete_name(flipped)
        
    # Fallback: use as-is but in proper case
    return format_athlete_name(raw_name)


def integrate_paris_data(athlete_bio: List[List[str]],
                         event_results: List[List[str]],
                         paris_athletes: List[List[str]],
                         paris_events: List[List[str]],
                         paris_medallists: List[List[str]],
                         paris_teams: List[List[str]],
                         paris_edition_id: str,
                         birth_dates: Dict[str, str]
                         ) -> Tuple[List[List[str]], List[List[str]], Dict[str, str]]:
    """
    Integrate all Paris 2024 data into the main athlete and event datasets,
    creating new rows where needed, updating existing records, and extending the
    birth_dates mapping with any new or improved birthdate information.

    This function merges multiple Paris-specific CSV files into the cleaned
    historical datasets by:
      1. Matching Paris athletes to existing bios (by normalized name + NOC).
      2. Creating new athlete records for genuinely new athletes and assigning
         new unique athlete_id values.
      3. Backfilling missing attributes (birthdate, height, weight, country) in
         existing bios when Paris data provides better information.
      4. Generating Paris 2024 event result rows (individual + team events) and
         appending them to olympic_athlete_event_results.csv.
      5. Ensuring medal information from paris_medallists is reflected in the
         event results, including medal-only records that might not appear in
         the raw Paris events list.
      6. Updating the shared birth_dates dictionary with new or corrected dates.

    Parameters
    ----------
    athlete_bio : List[List[str]]
        The cleaned rows of olympic_athlete_bio.csv (including header). This
        dataset is treated as the base to which Paris athletes are matched or
        added. Must contain at least:
        - 'athlete_id'
        - 'name'
        - 'sex'
        - 'born'
        - 'country_noc'
        Optional but used when present:
        - 'height'
        - 'weight'
        - 'country'

    event_results : List[List[str]]
        The cleaned rows of olympic_athlete_event_results.csv (including header).
        Paris 2024 event rows will be appended here. Must contain at least:
        - 'edition'
        - 'edition_id'
        - 'country_noc'
        - 'sport'
        - 'event'
        - 'result_id'
        - 'athlete'
        - 'athlete_id'
        - 'pos'
        - 'medal'
        - 'isTeamSport'

    paris_athletes : List[List[str]]
        Rows from the Paris 2024 athletes file (paris_athletes.csv). Expected
        columns include:
        - 'code'          : unique Paris athlete code
        - 'name'          : raw name string
        - 'gender'
        - 'country_code'  : NOC code
        - 'birth_date'
        - 'events'        : serialized list of event names
        These are used to either match existing athletes or create new ones.

    paris_events : List[List[str]]
        Rows from the Paris events file (paris_events.csv). Used to map:
        - event → sport
        Must contain at least 'event' and 'sport'. This mapping is used when
        constructing new event result rows for Paris 2024.

    paris_medallists : List[List[str]]
        Rows from the Paris medallists file (paris_medallists.csv). Used to
        attach medal information to Paris events. Expected columns:
        - 'code_athlete'
        - 'event'
        - 'medal_type'
        Medal info is merged into the generated event results and can also
        create additional “medal-only” events when needed.

    paris_teams : List[List[str]]
        Rows from the Paris team events file (paris_teams.csv). Used to create
        team event rows for Paris 2024. Expected columns include:
        - 'team'
        - 'country_code'
        - 'discipline'
        - 'events'
        - 'athletes'
        - 'athletes_codes'
        Each row describes a team, its members, and the events they competed in.

    paris_edition_id : str
        The edition_id corresponding to Paris 2024 (as determined by
        clean_games_data). This is written into all newly created event rows for
        the Paris 2024 Games.

    birth_dates : Dict[str, str]
        A mapping of athlete_id → birthdate ('dd-Mon-yyyy') produced by
        clean_athlete_data. This mapping is updated in-place and returned with
        additional entries for new Paris athletes and backfilled birthdates for
        existing athletes.

    Returns
    -------
    Tuple[List[List[str]], List[List[str]], Dict[str, str]]
        A tuple containing:

        1. **updated_bio** : List[List[str]]
           The athlete_bio dataset with:
           - New Paris athletes appended, each with a new unique athlete_id.
           - Existing rows enriched with Paris-derived data (birth, height,
             weight, country) when such fields were previously missing.

        2. **updated_events** : List[List[str]]
           The event_results dataset with:
           - New individual Paris 2024 event rows for each athlete/event pair
             in the Paris athlete data.
           - Team event rows derived from paris_teams, with isTeamSport set to
             "True".
           - Medal-only events derived from paris_medallists when an athlete
             has a medal but no corresponding event row yet.
           All new rows are tagged with:
             - edition = "2024 Summer Olympics"
             - edition_id = paris_edition_id

        3. **updated_birth_dates** : Dict[str, str]
           The birth_dates dictionary extended to include:
           - Birthdates for newly created Paris athletes (if parsable).
           - Backfilled/corrected birthdates for existing athletes where Paris
             supplies valid date information.

    Notes
    -----
    - Athlete matching uses a normalized key: `name.lower() + "_" + NOC`, with
      Paris names normalized via normalize_paris_name() to align with the main
      bio format.
    - New athlete_id and result_id values are generated by scanning existing
      maximum IDs in the input datasets and incrementing from there, ensuring
      no collisions.
    - Height and weight values of "0" in Paris data are treated as missing.
    - Multiple sets (e.g., team_event_seen, medal_event_seen) are used to avoid
      creating duplicate event rows per (athlete_id, event) combination.
    - The function is designed to be idempotent with respect to ID generation
      and duplicate checking: running it once on a given input should produce a
      stable, non-duplicated integration of Paris 2024 into the historical data.
    """

    print(f"Integrating Paris 2024 data (edition_id: {paris_edition_id})...") 
    
    # Get headers
    bio_header = athlete_bio[0]
    event_header = event_results[0]
    
    # Get indices for athlete_bio
    try:
        bio_id_idx = bio_header.index("athlete_id")
        bio_name_idx = bio_header.index("name")
        bio_sex_idx = bio_header.index("sex")
        bio_born_idx = bio_header.index("born")
        bio_noc_idx = bio_header.index("country_noc")
        
        # Event results indices
        event_edition_idx = event_header.index("edition")
        event_edition_id_idx = event_header.index("edition_id")
        event_noc_idx = event_header.index("country_noc")
        event_sport_idx = event_header.index("sport")
        event_event_idx = event_header.index("event")
        event_result_id_idx = event_header.index("result_id")
        event_athlete_idx = event_header.index("athlete")
        event_athlete_id_idx = event_header.index("athlete_id")
        event_pos_idx = event_header.index("pos")
        event_medal_idx = event_header.index("medal")
        event_team_idx = event_header.index("isTeamSport")
    except ValueError as e:
        print(f"Error finding column index: {e}")
        return athlete_bio, event_results, birth_dates
    
    # Optional athlete_bio columns
    try:
        bio_height_idx = bio_header.index("height")
    except ValueError:
        bio_height_idx = -1

    try:
        bio_weight_idx = bio_header.index("weight")
    except ValueError:
        bio_weight_idx = -1

    try:
        bio_country_idx = bio_header.index("country")
    except ValueError:
        bio_country_idx = -1

    # Build existing athlete lookup (name + noc)
    existing_athletes: Dict[str, str] = {}
    for row in athlete_bio[1:]:
        if len(row) > bio_noc_idx:
            name = row[bio_name_idx].strip().lower()
            noc = row[bio_noc_idx].strip().upper()
            if name and noc:
                key = f"{name}_{noc}"
                existing_athletes[key] = row[bio_id_idx]

    # --- PERF: map athlete_id -> row index for O(1) updates ---
    updated_bio = list(athlete_bio)
    id_to_bio_idx: Dict[str, int] = {}
    for idx, row in enumerate(updated_bio):
        if len(row) > bio_id_idx:
            aid = row[bio_id_idx].strip()
            if aid:
                id_to_bio_idx[aid] = idx
    
    # Find max IDs
    max_athlete_id = 0
    for row in athlete_bio[1:]:
        try:
            athlete_id = int(row[bio_id_idx].strip())
            if athlete_id > max_athlete_id:
                max_athlete_id = athlete_id
        except (ValueError, IndexError):
            pass
    
    max_result_id = 0
    for row in event_results[1:]:
        try:
            result_id = int(row[event_result_id_idx].strip())
            if result_id > max_result_id:
                max_result_id = result_id
        except (ValueError, IndexError):
            pass
    
    next_athlete_id = max_athlete_id + 1
    next_result_id = max_result_id + 1
    
    # Build event to sport mapping from paris_events
    event_to_sport: Dict[str, str] = {}
    if paris_events and len(paris_events) > 1:
        e_header = paris_events[0]
        try:
            e_event_idx = e_header.index("event")
            e_sport_idx = e_header.index("sport")
        except ValueError:
            e_event_idx = e_sport_idx = -1
        
        if e_event_idx >= 0 and e_sport_idx >= 0:
            for row in paris_events[1:]:
                if len(row) > e_sport_idx:
                    event_name = row[e_event_idx].strip()
                    sport_name = row[e_sport_idx].strip()
                    if event_name:
                        event_to_sport[event_name] = sport_name
    
    # Process Paris athletes
    if not paris_athletes or len(paris_athletes) < 2:
        print("No Paris athletes data found")
        return athlete_bio, event_results, birth_dates
    
    p_header = paris_athletes[0]
    try:
        p_code_idx = p_header.index("code")
        p_name_idx = p_header.index("name")
        p_gender_idx = p_header.index("gender")
        p_country_idx = p_header.index("country_code")
        p_birth_idx = p_header.index("birth_date")
        p_events_idx = p_header.index("events")
    except ValueError as e:
        print(f"Error finding Paris athlete column: {e}")
        return athlete_bio, event_results, birth_dates
    
    # Optional Paris athlete columns
    try:
        p_name_tv_idx = p_header.index("name_tv")
    except ValueError:
        p_name_tv_idx = -1

    try:
        p_country_name_idx = p_header.index("country")
    except ValueError:
        p_country_name_idx = -1

    try:
        p_height_idx = p_header.index("height")
    except ValueError:
        p_height_idx = -1

    try:
        p_weight_idx = p_header.index("weight")
    except ValueError:
        p_weight_idx = -1

    updated_events = list(event_results)
    
    # -----------------------------
    # Medallists: medal info + team events via code_team
    # -----------------------------
    team_event_names: Set[str] = set()
    medalist_info: Dict[Tuple[str, str], str] = {}

    if paris_medallists and len(paris_medallists) > 1:
        m_header = paris_medallists[0]
        try:
            m_code_idx = m_header.index("code_athlete")
            m_medal_idx = m_header.index("medal_type")
            m_event_idx = m_header.index("event")
        except ValueError:
            m_code_idx = m_medal_idx = m_event_idx = -1

        # code_team column (for team medals)
        try:
            m_code_team_idx = m_header.index("code_team")
        except ValueError:
            m_code_team_idx = -1
        
        if m_code_idx >= 0 and m_medal_idx >= 0 and m_event_idx >= 0:
            for row in paris_medallists[1:]:
                if len(row) <= max(m_code_idx, m_medal_idx, m_event_idx):
                    continue

                code = row[m_code_idx].strip()
                medal = row[m_medal_idx].strip()
                event_name = row[m_event_idx].strip()

                if code and medal and event_name:
                    medalist_info[(code, event_name)] = medal

                # If this medallist row has a code_team, this is a team event
                if 0 <= m_code_team_idx < len(row):
                    code_team = row[m_code_team_idx].strip()
                    if code_team and event_name:
                        team_event_names.add(event_name)
    
    athlete_count = 0
    event_count = 0
    
    # Create a mapping from athlete code to athlete info for faster lookups
    code_to_athlete_info: Dict[str, Dict[str, str]] = {}
    
    # Process ALL Paris athletes
    for row in paris_athletes[1:]:
        if len(row) < len(p_header):
            continue
        
        code = row[p_code_idx].strip()
        raw_name = row[p_name_idx].strip()
        alt_name = row[p_name_tv_idx].strip() if 0 <= p_name_tv_idx < len(row) and p_name_tv_idx < len(row) else ""
        name = normalize_paris_name(raw_name, alt_name)

        gender = row[p_gender_idx].strip() if p_gender_idx < len(row) else ""
        noc = row[p_country_idx].strip().upper() if p_country_idx < len(row) else ""
        country_name = row[p_country_name_idx].strip() if 0 <= p_country_name_idx < len(row) and p_country_name_idx < len(row) else ""

        height = row[p_height_idx].strip() if 0 <= p_height_idx < len(row) and p_height_idx < len(row) else ""
        weight = row[p_weight_idx].strip() if 0 <= p_weight_idx < len(row) and p_weight_idx < len(row) else ""
        birth = row[p_birth_idx].strip() if p_birth_idx < len(row) else ""
        events_str = row[p_events_idx].strip() if p_events_idx < len(row) else "[]"
        
        if not code or not name or not noc:
            continue

        # Treat '0' height/weight as unknown
        if height == "0":
            height = ""
        if weight == "0":
            weight = ""
        
        # Store athlete info for team event processing
        code_to_athlete_info[code] = {
            'name': name,
            'gender': gender,
            'country': noc,          # country_noc
            'birth': birth,
            'events_str': events_str,
            'height': height,
            'weight': weight,
            'country_name': country_name,
        }
        
        # Check if athlete already exists in main bio
        key = f"{name.lower()}_{noc}"
        athlete_id = existing_athletes.get(key)
        
        # Add athlete if not exists
        if not athlete_id:
            cleaned_birth = clean_birth_date_enhanced(birth)
            athlete_id = str(next_athlete_id)
            next_athlete_id += 1
            
            new_athlete = [""] * len(bio_header)
            new_athlete[bio_id_idx] = athlete_id
            new_athlete[bio_name_idx] = name
            new_athlete[bio_sex_idx] = "Male" if gender.lower().startswith("m") else "Female"
            new_athlete[bio_born_idx] = cleaned_birth
            new_athlete[bio_noc_idx] = noc

            if bio_height_idx >= 0:
                new_athlete[bio_height_idx] = height
            if bio_weight_idx >= 0:
                new_athlete[bio_weight_idx] = weight
            if bio_country_idx >= 0:
                new_athlete[bio_country_idx] = f" {country_name}" if country_name else ""
            
            # PERF: track row index for direct updates later
            idx = len(updated_bio)
            updated_bio.append(new_athlete)
            id_to_bio_idx[athlete_id] = idx

            existing_athletes[key] = athlete_id
            athlete_count += 1

            if cleaned_birth:
                birth_dates[athlete_id] = cleaned_birth
        else:
            # PERF: use id_to_bio_idx instead of scanning updated_bio
            row_idx = id_to_bio_idx.get(athlete_id)
            if row_idx is not None:
                bio_row = updated_bio[row_idx]
                # Birth
                if not bio_row[bio_born_idx] and birth:
                    cleaned_birth = clean_birth_date_enhanced(birth)
                    bio_row[bio_born_idx] = cleaned_birth
                    if cleaned_birth:
                        birth_dates[athlete_id] = cleaned_birth

                # Height/weight/country backfill if missing
                if bio_height_idx >= 0 and not bio_row[bio_height_idx] and height:
                    bio_row[bio_height_idx] = height
                if bio_weight_idx >= 0 and not bio_row[bio_weight_idx] and weight:
                    bio_row[bio_weight_idx] = weight
                if bio_country_idx >= 0 and not bio_row[bio_country_idx] and country_name:
                    bio_row[bio_country_idx] = country_name
        
        # Store athlete_id for code lookup
        code_to_athlete_info[code]['athlete_id'] = athlete_id
        
        # Parse events list
        try:
            events_clean = events_str.replace("'", '"')
            events_list = ast.literal_eval(events_clean)
            if not isinstance(events_list, list):
                events_list = [events_list]
        except (ValueError, SyntaxError):
            # Simple manual fallback
            events_clean = events_str.strip("[]")
            events_list = [e.strip().strip('"\'') for e in events_clean.split(",") if e.strip()]
        
        for event_name in events_list:
            if not event_name:
                continue
            
            sport = event_to_sport.get(event_name, "Unknown")
            
            medal = ""
            pos = ""
            medal_type = medalist_info.get((code, event_name))
            if medal_type:
                mt = medal_type.lower()
                if "gold" in mt:
                    medal, pos = "Gold", "1"
                elif "silver" in mt:
                    medal, pos = "Silver", "2"
                elif "bronze" in mt:
                    medal, pos = "Bronze", "3"
            
            new_event = [""] * len(event_header)
            new_event[event_edition_idx] = "2024 Summer Olympics"
            new_event[event_edition_id_idx] = paris_edition_id
            new_event[event_noc_idx] = noc
            new_event[event_sport_idx] = sport
            new_event[event_event_idx] = event_name
            new_event[event_result_id_idx] = str(next_result_id)
            next_result_id += 1
            new_event[event_athlete_idx] = name
            new_event[event_athlete_id_idx] = athlete_id
            new_event[event_pos_idx] = pos
            new_event[event_medal_idx] = medal

            # team / relay events from code_team in paris_medallists
            is_team_event = event_name in team_event_names
            new_event[event_team_idx] = "True" if is_team_event else "False"
            
            updated_events.append(new_event)
            event_count += 1
    
    print(f"Added {athlete_count} Paris athletes and {event_count} individual events")
    
    # --- TEAM EVENTS ---
    if paris_teams and len(paris_teams) > 1:
        t_header = paris_teams[0]
        try:
            t_team_idx = t_header.index("team")
            t_country_idx = t_header.index("country_code")
            t_discipline_idx = t_header.index("discipline")
            t_event_idx = t_header.index("events")
            t_athletes_idx = t_header.index("athletes")
            t_athletes_codes_idx = t_header.index("athletes_codes")
        except ValueError:
            t_team_idx = t_country_idx = t_discipline_idx = t_event_idx = t_athletes_idx = t_athletes_codes_idx = -1
        
        if (t_team_idx >= 0 and t_country_idx >= 0 and t_discipline_idx >= 0 and 
            t_event_idx >= 0 and t_athletes_idx >= 0 and t_athletes_codes_idx >= 0):
            
            team_count = 0
            # PERF: track team events we've added
            team_event_seen: Set[Tuple[str, str]] = set()
            
            for row in paris_teams[1:]:
                if len(row) <= max(t_team_idx, t_country_idx, t_discipline_idx, 
                                   t_event_idx, t_athletes_idx, t_athletes_codes_idx):
                    continue
                
                team_name = row[t_team_idx].strip()
                country = row[t_country_idx].strip().upper()
                discipline = row[t_discipline_idx].strip()
                event_name = row[t_event_idx].strip()
                athletes_str = row[t_athletes_idx].strip()
                athlete_codes_str = row[t_athletes_codes_idx].strip()
                
                if not event_name or not country:
                    continue
                
                try:
                    athletes_clean = athletes_str.replace("'", '"')
                    codes_clean = athlete_codes_str.replace("'", '"')
                    athletes_list = ast.literal_eval(athletes_clean)
                    athlete_codes_list = ast.literal_eval(codes_clean)
                except (ValueError, SyntaxError):
                    continue
                
                for i, athlete_name in enumerate(athletes_list):
                    if i >= len(athlete_codes_list):
                        break
                    athlete_code = athlete_codes_list[i]
                    
                    athlete_name = format_athlete_name(athlete_name)

                    # Find athlete ID using code_to_athlete_info mapping
                    athlete_id = None
                    info = code_to_athlete_info.get(athlete_code)
                    if info:
                        athlete_id = info.get("athlete_id")
                    else:
                        key = f"{athlete_name.lower()}_{country}"
                        athlete_id = existing_athletes.get(key)
                    
                    if not athlete_id:
                        continue
                    
                    key_ev = (athlete_id, event_name)
                    if key_ev in team_event_seen:
                        continue
                    team_event_seen.add(key_ev)
                    
                    new_event = [""] * len(event_header)
                    new_event[event_edition_idx] = "2024 Summer Olympics"
                    new_event[event_edition_id_idx] = paris_edition_id
                    new_event[event_noc_idx] = country
                    new_event[event_sport_idx] = discipline
                    new_event[event_event_idx] = event_name
                    new_event[event_result_id_idx] = str(next_result_id)
                    next_result_id += 1
                    new_event[event_athlete_idx] = athlete_name
                    new_event[event_athlete_id_idx] = athlete_id
                    new_event[event_pos_idx] = ""
                    new_event[event_medal_idx] = ""
                    new_event[event_team_idx] = "True"
                    
                    updated_events.append(new_event)
                    event_count += 1
                    team_count += 1
            
            print(f"Added {team_count} team events")
    
    # --- MEDAL-ONLY EVENTS (for any left-over medallists) ---
    if paris_medallists and len(paris_medallists) > 1:
        m_header = paris_medallists[0]
        try:
            m_code_idx = m_header.index("code_athlete")
            m_name_idx = m_header.index("name")
            m_gender_idx = m_header.index("gender")
            m_country_idx = m_header.index("country_code")
            m_event_idx = m_header.index("event")
            m_medal_idx = m_header.index("medal_type")
        except ValueError:
            m_code_idx = m_name_idx = m_gender_idx = m_country_idx = m_event_idx = m_medal_idx = -1
        
        if all(idx >= 0 for idx in [m_code_idx, m_name_idx, m_country_idx, m_event_idx, m_medal_idx]):
            medal_only_count = 0
            # PERF: track medal events we've already added
            medal_event_seen: Set[Tuple[str, str]] = set()
            
            for row in paris_medallists[1:]:
                if len(row) <= max(m_code_idx, m_name_idx, m_country_idx, m_event_idx, m_medal_idx):
                    continue
                
                code = row[m_code_idx].strip()
                raw_medal_name = row[m_name_idx].strip()
                name = format_athlete_name(raw_medal_name)
                country = row[m_country_idx].strip().upper()
                event_name = row[m_event_idx].strip()
                medal_type = row[m_medal_idx].strip().lower()
                
                if not code or not name or not country or not event_name:
                    continue
                
                # Find athlete ID
                athlete_id = None
                info = code_to_athlete_info.get(code)
                if info:
                    athlete_id = info.get("athlete_id")
                else:
                    key = f"{name.lower()}_{country}"
                    athlete_id = existing_athletes.get(key)
                
                if not athlete_id:
                    # Create new athlete (rare path)
                    athlete_id = str(next_athlete_id)
                    next_athlete_id += 1
                    
                    new_athlete = [""] * len(bio_header)
                    new_athlete[bio_id_idx] = athlete_id
                    new_athlete[bio_name_idx] = name
                    # crude gender guess: if row gender exists, use it; else, default
                    gender_val = row[m_gender_idx].strip().lower() if m_gender_idx >= 0 and m_gender_idx < len(row) else ""
                    if gender_val.startswith("m"):
                        new_athlete[bio_sex_idx] = "Male"
                    elif gender_val.startswith("f"):
                        new_athlete[bio_sex_idx] = "Female"
                    else:
                        new_athlete[bio_sex_idx] = ""
                    new_athlete[bio_born_idx] = ""
                    new_athlete[bio_noc_idx] = country
                    
                    idx = len(updated_bio)
                    updated_bio.append(new_athlete)
                    id_to_bio_idx[athlete_id] = idx
                    existing_athletes[f"{name.lower()}_{country}"] = athlete_id
                    athlete_count += 1
                
                key_ev = (athlete_id, event_name)
                if key_ev in medal_event_seen:
                    continue
                medal_event_seen.add(key_ev)
                
                sport = event_to_sport.get(event_name, "Unknown")
                
                medal = ""
                pos = ""
                if "gold" in medal_type:
                    medal, pos = "Gold", "1"
                elif "silver" in medal_type:
                    medal, pos = "Silver", "2"
                elif "bronze" in medal_type:
                    medal, pos = "Bronze", "3"
                
                new_event = [""] * len(event_header)
                new_event[event_edition_idx] = "2024 Summer Olympics"
                new_event[event_edition_id_idx] = paris_edition_id
                new_event[event_noc_idx] = country
                new_event[event_sport_idx] = sport
                new_event[event_event_idx] = event_name
                new_event[event_result_id_idx] = str(next_result_id)
                next_result_id += 1
                new_event[event_athlete_idx] = name
                new_event[event_athlete_id_idx] = athlete_id
                new_event[event_pos_idx] = pos
                new_event[event_medal_idx] = medal

                # team flag for medal-only rows as well
                is_team_event = event_name in team_event_names
                new_event[event_team_idx] = "True" if is_team_event else "False"
                
                updated_events.append(new_event)
                event_count += 1
                medal_only_count += 1
            
            if medal_only_count > 0:
                print(f"Added {medal_only_count} additional medal events")
    
    print(f"Total Paris athletes: {athlete_count}, Total events: {event_count}")
    return updated_bio, updated_events, birth_dates

# ==================== MAIN FUNCTION by Gurjeet  ====================

def main() -> None:
    """
    Orchestrate the complete Olympics data cleaning and Paris 2024 integration
    workflow, producing all required Milestone 2 output CSV files.

    This function coordinates every stage of the pipeline:

    1. **Load original datasets**
       Reads the historical Olympic datasets:
         - olympic_athlete_bio.csv
         - olympic_athlete_event_results.csv
         - olympics_country.csv
         - olympics_games.csv

    2. **Load Paris 2024 datasets**
       Reads the Paris-specific files from the repository (athletes, events,
       medallists, teams, NOCs).

    3. **Clean core datasets**
       - Clean athlete bios and extract birthdates.
       - Clean Games date fields and detect the Paris 2024 edition_id.
       - Clean and unify country/NOC mappings.

    4. **Integrate Paris 2024 data**
       Using the cleaned datasets and the Paris edition_id, merge Paris athletes,
       events, team events, medallists, and backfilled attributes into the main
       datasets.

    5. **Add derived metrics**
       - Add athlete ages to event results based on birthdates and competition
         windows.
       - Generate the medal tally summary file.

    6. **Write output CSV files**
       Produces all cleaned and derived Milestone 2 deliverables:
         - new_athlete_bio.csv
         - new_athlete_event_results.csv
         - new_countries.csv
         - new_games.csv
         - new_age.csv
         - new_medal_tally.csv

    Notes
    -----
    - This function performs no return; it writes finalized datasets to disk.
    - All processing functions it calls are designed to be idempotent and safe
      against malformed or incomplete input rows.
    - The workflow intentionally separates cleaning, integrating, and generating
      derived data to enforce clarity and reproducibility for Milestone grading.
    """

    start_time = time.time()
    print("Starting project processing...")
    
    # Read files
    print("Reading files...")
    athlete_bio = read_csv_file("olympic_athlete_bio.csv")
    event_results = read_csv_file("olympic_athlete_event_results.csv")
    countries = read_csv_file("olympics_country.csv")
    games = read_csv_file("olympics_games.csv")
    
    # Read Paris files
    print("Reading Paris files...")
    paris_athletes = read_csv_file("paris/athletes.csv")
    paris_events = read_csv_file("paris/events.csv")
    paris_medallists = read_csv_file("paris/medallists.csv")
    paris_nocs = read_csv_file("paris/nocs.csv")
    paris_teams = read_csv_file("paris/teams.csv")
    
    print(f"Original athlete count: {len(athlete_bio)-1}")
    print(f"Original event count: {len(event_results)-1}")
    print(f"Paris athletes available: {len(paris_athletes)-1}")
    print(f"Paris events available: {len(paris_events)-1}")
    
    # Step 1: Clean athlete bio
    print("\nStep 1: Cleaning athlete bio...")
    clean_athletes, birth_dates, athlete_name_noc_map = clean_athlete_data(athlete_bio)
    
    # Step 2: Clean games and get Paris edition_id
    print("Step 2: Cleaning games data...")
    clean_games, paris_edition_id = clean_games_data(games)
    print(f"Paris edition_id: {paris_edition_id}")
    
    # Step 3: Clean countries
    print("Step 3: Processing countries...")
    clean_countries_data = clean_countries(countries, paris_nocs)
    
    # Step 4: Integrate Paris data
    print("Step 4: Integrating Paris 2024 data...")
    athletes_with_paris, events_with_paris, birth_dates = integrate_paris_data(
    clean_athletes, event_results,
    paris_athletes, paris_events, paris_medallists, paris_teams,
    paris_edition_id,
    birth_dates,
    )
    
    print(f"Athlete count after Paris integration: {len(athletes_with_paris)-1}")
    print(f"Event count after Paris integration: {len(events_with_paris)-1}")
    
    # Step 5: Add age column
    print("Step 5: Adding age column...")
    events_with_age = add_age_to_events(events_with_paris, birth_dates, clean_games)
    
    # Count how many events have age
    age_count = sum(1 for row in events_with_age[1:] if row[-1])
    print(f"Events with age calculated: {age_count}/{len(events_with_age)-1}")
    
    # Step 6: Generate medal tally
    print("Step 6: Generating medal tally...")
    medal_tally = generate_medal_tally(events_with_age, clean_countries_data, clean_games)
    
    # Write output files
    print("\nStep 7: Writing output files...")
    write_csv_file("new_olympic_athlete_bio.csv", athletes_with_paris)
    write_csv_file("new_olympic_athlete_event_results.csv", events_with_age)
    write_csv_file("new_olympics_country.csv", clean_countries_data)
    write_csv_file("new_olympics_games.csv", clean_games)
    write_csv_file("new_medal_tally.csv", medal_tally)
    
    end_time = time.time()
    print(f"\nProcessing completed in {end_time - start_time:.2f} seconds")
    
        # Validation summary - FIXED VERSION
    print("\n=== VALIDATION SUMMARY ===")
    print(f"Athlete bio entries: {len(athletes_with_paris)-1}")
    print(f"Event result entries: {len(events_with_age)-1}")
    print(f"Country entries: {len(clean_countries_data)-1}")
    print(f"Games entries: {len(clean_games)-1}")
    print(f"Medal tally entries: {len(medal_tally)-1}")
    
    # Check for Paris 2024 in medal tally - FAST VERSION
    paris_entries = 0
    for row in medal_tally[1:]:
        if "2024 Summer Olympics" in str(row[0]):
            paris_entries += 1
    print(f"Paris 2024 medal tally entries: {paris_entries}")
    
    # Count Paris athletes - FAST VERSION
    paris_athlete_ids = set()
    for event_row in events_with_age[1:5000]:  # Check first 5000 rows only
        if len(event_row) > 7 and "2024 Summer Olympics" in str(event_row[0]):
            athlete_id = event_row[7]  # athlete_id column
            if athlete_id:
                paris_athlete_ids.add(athlete_id)
    
    print(f"Paris 2024 athletes (sample): {len(paris_athlete_ids)}")
    
    # Sample check of dates
    print("\nSample birth dates (first 5 cleaned):")
    count = 0
    for row in athletes_with_paris[1:]:
        if len(row) > 3 and row[3]:
            print(f"  Athlete {count+1}: {row[3]}")
            count += 1
            if count >= 5:
                break
    
    # Check games dates
    print("\nSample game dates (Paris and Milano):")
    for row in clean_games[-5:]:
        if len(row) > 7:
            print(f"  {row[0]}: {row[7]} to {row[8] if len(row) > 8 else ''}")
if __name__ == "__main__":
    main()