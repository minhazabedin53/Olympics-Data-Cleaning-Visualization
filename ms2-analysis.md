# Milestone 2 – Analysis and Application Description

## 1. Assumptions and Design Decisions

### 1.1 Reconciling Paris 2024 with Historical Data

**Different name formats**

The historical file `olympic_athlete_bio.csv` stores names in a “First Last” style (with mixed casing), while the Paris files use multiple conventions:

* `paris_athletes.csv`:

  * `name` often in reversed form (`"ALEKSANYAN Artur"`),
  * `name_tv` in broadcast style (`"Artur ALEKSANYAN"`).
* `paris_medallists.csv` and `paris_teams.csv`:

  * Names can appear in uppercase, mixed case, and sometimes with multiple given names.

To reconcile this and reduce duplicates:

* We use `format_athlete_name(name)` to:

  * Lowercase the entire string,
  * Then capitalize each word,
  * Handling simple hyphenated parts (e.g., `"van-der"` → `"Van-Der"`).

* We use `normalize_paris_name(raw_name, alt_display)`:

  * If `alt_display` (`name_tv`) is present, We treat it as the most reliable display name and normalize it with `format_athlete_name`.
  * If only `raw_name` exists and matches a pattern like `"LAST First"` (`first token all caps, second token not all caps`), We flip to `"First LAST"` then normalize.
  * For longer names, we preserve the order unless they match the “all caps last name, non-caps rest” pattern; in that case we move the first all-caps token to the end.
  * Everything is normalized to consistent casing, so names from Paris and the main bio file are as comparable as possible.

* To detect duplicates when integrating Paris 2024, athletes are matched by:

  ```python
  key = f"{name.lower()}_{noc}"
  existing_athletes[key] = athlete_id
  ```

  This is a compromise: it’s robust and fast, but if names change order or spelling dramatically, they may still be treated as distinct athletes.

**Edition and games date assumptions**

* `clean_games_data()` treats the **edition year** as the ground truth and corrects messy date strings accordingly:

  * Single dates are normalized via `clean_single_games_date_enhanced(date_str, year)`.
  * Ranges or complex dates are normalized via `clean_games_date_enhanced(date_str, year)`.
* For **Paris 2024**:

  * If a row has `year == 2024` and either the city contains `"paris"` or the edition text contains `"2024"`, it is treated as the Paris 2024 edition.
  * Paris dates are **forced** to:

    * Start: `26-Jul-2024`
    * End: `11-Aug-2024`
  * This overwrites irregular source dates so that all later logic (age calculation, comparisons) uses consistent dates.
  * The `edition_id` for this row is captured and returned as `paris_edition_id`.

**How Paris data is merged into historical data**

The integration is centralized in `integrate_paris_data()`:

* We treat the cleaned historical `olympic_athlete_bio.csv` as the base.

* For each row in `paris_athletes.csv`:

  1. Normalize the name (`normalize_paris_name`).
  2. Look up `(normalized_name.lower(), NOC)` in `existing_athletes`:

     * If found, we treat the Paris athlete as the same person and **update** missing fields (birth, height, weight, country).
     * If not found, we allocate a new `athlete_id` and append a new row to `new_olympic_athlete_bio.csv`.

* Paris event results are **not** taken from a raw Paris events results file; instead, they are synthesized by combining:

  * `paris_athletes.csv` (who + which events),
  * `paris_events.csv` (event → sport mapping),
  * `paris_medallists.csv` (who actually won which medal),
  * `paris_teams.csv` (team composition for team sports).

* New event rows for Paris are added to `new_olympic_athlete_event_results.csv` with:

  * `edition = "2024 Summer Olympics"`
  * `edition_id = paris_edition_id`
  * `country_noc` from Paris `country_code`
  * `sport` from `paris_events` via `event_to_sport[event_name]`
  * `event` from the Paris event list
  * `athlete` = normalized athlete name
  * `athlete_id` = matched or newly created ID
  * `medal` / `pos` derived from `paris_medallists` when applicable
  * `isTeamSport` determined as described below.

### 1.2 Handling Missing and Inconsistent Data

**Birthdates**

* `clean_birth_date_enhanced()` is used in `clean_athlete_data()` and in Paris integration to normalize all birthdates to `'dd-Mon-yyyy'`.
* It supports:

  * `dd-Mon-yy`, `dd-Mon-yyyy`,
  * `dd Month yyyy` (full month name),
  * Year-only and fuzzy formats such as `"1884"`, `"(1884)"`, `"c. 1884"`, `"circa 1884"`, etc.
* When only a year can be extracted, we assume `01-Jan-<year>` as a placeholder. This is an explicit assumption that allows:

  * consistent formatting,
  * meaningful age calculations (approximate but better than omitting the age entirely),
  * while still preserving the year of birth, which is usually the most important aspect.

**Games dates**

* `clean_games_data()` pads rows to the header length, then:

  * Normalizes `start_date`, `end_date`, `competition_date` using `clean_games_date_enhanced` and `clean_single_games_date_enhanced`.
  * Ensures all competition-related dates follow a consistent `'dd-Mon-yyyy'` format.

* Age calculation always expects clean dates; invalid or missing dates propagate as empty ages (`""`).

**Height, weight, country for Paris athletes**

From `paris_athletes.csv`:

* `height` and `weight`:

  * `"0"` is treated as “unknown” and stored as an empty string.
* `country_code` becomes `country_noc`.
* `country` (long name) is written into the `country` column if present.

If the athlete already exists in the main bio file:

* We only **backfill missing values**:

  * If `born` is empty and Paris provides a date, we clean and fill it.
  * If `height` / `weight` / `country` are empty and Paris provides non-empty values, we update them.
* We **do not** overwrite existing non-empty historical data.

**Team sports and medal counting**

A major design decision is to identify team sports using `code_team` in `paris_medallists.csv`:

* While building medallist info, we collect:

  * `medalist_info[(code_athlete, event)] = medal_type`
  * `team_event_names = {event_name | there exists a row with non-empty code_team and this event}`

* Later, when constructing Paris event rows, we set:

  * `isTeamSport = "True"` if `event_name in team_event_names`
  * `isTeamSport = "False"` otherwise.

In `generate_medal_tally()`:

* we keep the original row-based counting logic for **all historical editions** (one medal per event row).
* For **Paris 2024 team events only**, we add a small correction:

  * we use a set `seen_paris_team_medals` keyed by `(edition_id, NOC, event_name, medal)` and only count each unique team medal **once per country per event**, even though there may be multiple event rows (one per athlete) with the same medal.

This assumes:

* The instructor’s expected tally is based on official team medals, not “number of medalists,” while still preserving historical behavior for older editions.

---

## 2. Data Structures Used

The code uses only Python’s **built-in data structures**: lists, dictionaries, sets, and tuples. No custom container classes are implemented; instead, we focused on picking the right built-ins and combining them effectively.

### 2.1 CSV Tables as `List[List[str]]`

All CSV files are loaded and stored as:

* `List[List[str]]` where:

  * Row 0 is the header.
  * Rows 1…N are data rows, each a list of strings.

This representation is used consistently for:

* `olympic_athlete_bio.csv`
* `olympic_athlete_event_results.csv`
* `olympics_country.csv`
* `olympics_games.csv`
* `paris_athletes.csv`
* `paris_events.csv`
* `paris_medallists.csv`
* `paris_teams.csv`

**Why?**

* It mirrors CSV closely and works nicely with `csv.reader` / `csv.writer`.
* It keeps memory use simple and predictable.
* Column access is done by:

  * Finding indices once (`header.index("medal")`, etc.), then
  * Using integer indexing (`row[medal_idx]`) inside loops.

This is O(1) per field access and easy to reason about.

### 2.2 Dictionaries (`dict`)

Dictionaries are used to avoid repeated scanning and to join data across files efficiently.

Key dictionaries:

1. **Athlete-level maps in `clean_athlete_data()`**

   * `birth_dates[athlete_id] = cleaned_birthdate`
     Used later by `add_age_to_events()` to compute ages.
   * `athlete_name_noc_map[f"{name}_{noc}"] = athlete_id`
     Designed to support duplicate detection when integrating Paris data.

2. **Athlete duplicate detection and updates in `integrate_paris_data()`**

   * `existing_athletes[f"{name.lower()}_{noc}"] = athlete_id`
     Built from the cleaned main bio file; used to decide whether to reuse an athlete or create a new one.
   * `id_to_bio_idx[athlete_id] = row_index`
     Maps each ID to its row index in the `updated_bio` list so we can update rows in **O(1)** instead of scanning.
   * `code_to_athlete_info[code_athlete] = { 'name': ..., 'gender': ..., 'country': ..., 'birth': ..., 'events_str': ..., 'height': ..., 'weight': ..., 'country_name': ..., 'athlete_id': ... }`
     Used for both individual and team event generation.

3. **Joining and lookups across files**

   * `event_to_sport[event_name] = sport_name` (from `paris_events.csv`)
     Ensures consistent sport names for Paris events.
   * `noc_to_country[noc] = country_name` (from `olympics_country.csv` + `paris_nocs.csv`)
     Ensures a single canonical country name per NOC.
   * `edition_id_to_name[edition_id] = edition` (from `olympics_games.csv`)
     Used in `generate_medal_tally()` to get full edition names.
   * `medalist_info[(code_athlete, event_name)] = medal_type`
     Allows `integrate_paris_data()` to assign medals and positions when creating Paris event rows.

4. **Medal tally aggregation in `generate_medal_tally()`**

   ```python
   tally[(edition_id, noc)] = {
       "edition": edition,
       "athletes": set(),
       "gold": 0,
       "silver": 0,
       "bronze": 0,
   }
   ```

   * This accumulates medal and athlete counts per edition and NOC.
   * Dictionary updates are O(1), making the entire aggregation linear in the number of event rows.

### 2.3 Sets (`set`)

Sets are used wherever we need **uniqueness** or deduplication.

Important sets:

1. **Unique athletes per edition/NOC**

   * `tally[(edition_id, noc)]["athletes"]` is a `set` of `athlete_id`s.
   * This ensures `number_of_athletes` in the medal tally counts each athlete once per edition/NOC, no matter how many events they appear in.

2. **Team event names (Paris)**

   * `team_event_names = {event_name | there is a medallist row with non-empty code_team for this event}`.
   * Checking whether an event is team-based is then a simple O(1) `event_name in team_event_names`.

3. **Avoiding duplicate team events & medals**

   * `team_event_seen = {(athlete_id, event_name)}`
     Ensures each team event row is created once per athlete when processing `paris_teams.csv`.
   * `seen_paris_team_medals = {(edition_id, noc, event_name, medal)}`
     Ensures each Paris team medal is only counted once in the medal tally.
   * `medal_event_seen = {(athlete_id, event_name)}` in the medal-only backfill
     Prevents duplicate medal-only event rows for the same athlete/event.

### 2.4 Tuples

Tuples are mostly used as **keys** in dictionaries and sets for compound identities:

* `(edition_id, noc)` → medal tally bucket.
* `(code_athlete, event_name)` → medallist info.
* `(athlete_id, event_name)` → unique team event per athlete.
* `(edition_id, noc, event_name, medal)` → unique team medal per country/event.

Tuples are immutable and hashable, which makes them ideal as dictionary/set keys.

---

## 3. General Data Processing Flow

The `main()` function orchestrates the entire workflow. In high-level steps:

1. **Load original datasets**

   Using `read_csv_file()`:

   * `olympic_athlete_bio.csv` → `athlete_bio`
   * `olympic_athlete_event_results.csv` → `event_results`
   * `olympics_country.csv` → `countries`
   * `olympics_games.csv` → `games`

2. **Load Paris 2024 datasets**

   From the `paris` folder (with paths updated in `main()` as needed):

   * `paris_athletes.csv`
   * `paris_events.csv`
   * `paris_medallists.csv`
   * `paris_nocs.csv`
   * `paris_teams.csv`

3. **Clean athlete bios and build birthdate mappings**

   * `clean_athlete_data(athlete_bio)` returns:

     * `clean_athletes` (normalized bios),
     * `birth_dates[athlete_id] = cleaned_birth_date`,
     * `athlete_name_noc_map` (for duplicate detection).
   * These cleaned bios and birth dates become the foundation for age calculations and Paris integration.

4. **Clean games data and find Paris edition**

   * `clean_games, paris_edition_id = clean_games_data(games)`:

     * Normalizes all games dates.
     * Identifies the `edition_id` for Paris 2024 based on year, city, and edition text.
     * Forces Paris dates to `26-Jul-2024` to `11-Aug-2024`.

5. **Clean and consolidate country/NOC data**

   * `clean_countries_data = clean_countries(countries, paris_nocs)`:

     * Builds a combined NOC list from historical data and Paris NOCs.
     * Deduplicates and sorts by country name.

6. **Integrate Paris 2024 data**

   * `athletes_with_paris, events_with_paris, birth_dates = integrate_paris_data(...)`:

     * Matches or adds Paris athletes to `clean_athletes`.
     * Updates `birth_dates` for new or improved birth dates.
     * Builds event entries for Paris:

       * Individual events from `paris_athletes` + `paris_events` + `paris_medallists`.
       * Team events from `paris_teams`.
       * Medal-only events from `paris_medallists` for any remaining unmatched athletes/events.
     * Ensures `isTeamSport` is set correctly based on `team_event_names`.

7. **Add age column to event results**

   * `events_with_age = add_age_to_events(events_with_paris, birth_dates, clean_games)`:

     * Builds a mapping `edition_id → event_date` (competition_date or start/end range).
     * For each event row:

       * Looks up `athlete_id → birth_date` via `birth_dates`.
       * Looks up `edition_id → event_date` via `edition_dates`.
       * Computes age via `calculate_age(birth_date, event_date)` and appends it as the last column.

8. **Generate medal tally**

   * `medal_tally = generate_medal_tally(events_with_age, clean_countries_data, clean_games)`:

     * Aggregates medals and distinct athletes per `(edition_id, NOC)`.
     * Uses special team-medal deduplication logic **only** for Paris 2024.
     * Produces `new_medal_tally.csv` with the exact header required in the assignment.

9. **Write output files**

   Using `write_csv_file()`:

   * `new_olympic_athlete_bio.csv`
   * `new_olympic_athlete_event_results.csv`
   * `new_olympics_country.csv`
   * `new_olympics_games.csv`
   * `new_medal_tally.csv`

   Debug prints in `main()` (e.g., sample birth dates, sample game dates, count of events with age) are for validation but do not affect the outputs.

---

## 4. Runtime Analysis

Let:

* **n** = number of records in `olympic_athlete_event_results.csv` (historical events).
* **a** = number of records in `olympic_athlete_bio.csv`.
* **p** = number of records in `paris_athletes.csv`.
* **e** = number of records in `paris_events.csv`.
* **m** = number of records in `paris_medallists.csv`.

Other files (`olympics_games`, `olympics_country`, `paris_nocs`, `paris_teams`) are smaller and contribute only lower-order terms.

### 4.1 Runtime to Clean All Data

**Athlete data cleaning**

* `clean_athlete_data()` iterates once over all **a** rows.
* Each row:

  * Is padded to header length if necessary.
  * Gets its `born` field cleaned via `clean_birth_date_enhanced()`, which does a constant amount of parsing and regex work.
* It also populates two dictionaries (`birth_dates`, `athlete_name_noc_map`) in O(1) per row.
* Complexity: **O(a)**.

**Games data cleaning**

* `clean_games_data()` iterates once over the games table, say **g** rows.
* For each row:

  * It normalizes `start_date`, `end_date`, and/or `competition_date` using `clean_single_games_date_enhanced()` and `clean_games_date_enhanced()` (both O(1) per call).
  * For year 2024 rows, it applies special Paris logic.
* Complexity: **O(g)**.

**Country/NOC cleaning**

* `clean_countries()`:

  * Scans `countries` (≈ **c** rows) into a dict.
  * Scans `paris_nocs` into the same dict, only adding new NOCs.
  * Sorts the resulting NOC→country items by country name.
* Complexity: **O(c + |paris_nocs| + k log k)** where k is the number of unique NOCs. Since k and c are small compared to n and a, this is a lower-order term.

**(Optional) Event date cleaning**

* If event dates are cleaned per row (depending on dataset), that is a single pass over **n** rows with O(1) work per row.

**Total cleaning runtime**

Dominant terms:

> **O(a + n)**
> plus smaller contributions from games and country cleaning.

### 4.2 Runtime to Add Paris Data into the Records

Inside `integrate_paris_data()`:

1. **Precomputation from existing data**

   * Build `existing_athletes` and `id_to_bio_idx` by scanning `clean_athletes`:

     * Complexity: **O(a)**.
   * Build `event_to_sport` by scanning `paris_events.csv`:

     * Complexity: **O(e)**.

2. **Medallist maps and team event detection**

   * Single pass over `paris_medallists.csv` to fill:

     * `medalist_info[(code_athlete, event)]`
     * `team_event_names` (from rows with non-empty `code_team`).
   * Complexity: **O(m)**.

3. **Integrating Paris athletes**

   * Single pass over `paris_athletes.csv` (p rows):

     * Normalize name (O(1)),
     * Lookup in `existing_athletes` (O(1)),
     * Either update existing row via `id_to_bio_idx` (O(1)) or append new row and assign new ID (O(1)),
     * Clean and store birth date if present (O(1)),
     * Update `birth_dates` and `code_to_athlete_info`.

   * Each row may have one or more events. Let the total number of Paris athlete→event associations be `P_ev`. Parsing and adding events is O(1) per event association due to dictionary lookups (`medalist_info`, `event_to_sport`).

   * Complexity: **O(p + P_ev)**.

4. **Integrating Paris team events**

   * Single pass over `paris_teams.csv` (t rows):

     * Parse the athletes and codes lists (a small constant factor per team).
     * For each athlete in the team, lookup via `code_to_athlete_info` or `existing_athletes`.
     * Insert event rows, with deduplication via `team_event_seen`.

   * Complexity: **O(t * average_team_size)**, normally much smaller than n.

5. **Medal-only backfill**

   * Second pass over `m` rows in `paris_medallists.csv` for rare cases where medallists are not covered by the athlete/events parsing.
   * All operations within this loop are O(1) (dict/set lookups and updates).
   * Complexity: **O(m)**.

Overall integration complexity:

> **O(a + e + m + p + P_ev + t·team_size)**

Since `P_ev` is proportional to the number of new Paris event rows added, we can think of it as:

> **O(a + e + m + p + n_paris)**

where `n_paris` is the number of new event rows for Paris.

### 4.3 Runtime to Generate Medal Results for All Games

`generate_medal_tally(event_rows, country_rows, games_rows)` has three main phases:

1. **Build lookups**

   * `noc_to_country` from `country_rows` (≈ c rows) → **O(c)**.
   * `edition_id_to_name` from `games_rows` (≈ g rows) → **O(g)**.

2. **Aggregate over event rows**

   * The input `event_rows` includes all historical events (**n**) plus all newly added Paris events (**n_paris**). Let:

     > `n_total = n + n_paris`

   * For each event row:

     * Normalize and read the relevant fields.
     * Update `tally[(edition_id, noc)]` in O(1).
     * Add `athlete_id` to the `athletes` set (amortized O(1)).
     * For Paris 2024 team events, check and update `seen_paris_team_medals`.

   * Complexity: **O(n_total)**.

3. **Build and sort final result**

   * Iterate over all keys in `tally` (let this be K = number of unique `(edition_id, NOC)` combinations).
   * Sort them by edition_id (numerically where possible) and NOC: **O(K log K)**.
   * K is much smaller than `n_total` because many event rows share the same edition/NOC pair.

Total medal tally runtime:

> **O(n_total + K log K)**

In practice, `K << n_total`, so this is effectively **O(n_total)**, i.e., linear in the number of event rows with a relatively small sorting overhead at the end.

---

Overall, the key design choice is to keep all major steps **linear** in the size of their input datasets by:

* Using lists for sequential scans of CSV rows,
* Using dictionaries and sets wherever there is repeated lookup or cross-file joining,
* Avoiding nested loops over large tables.

That’s why even with full Paris integration and medal tally generation across all editions, your program still runs comfortably within the timing limits, while satisfying the data cleaning, integration, and data generation requirements for Milestone 2.


# Individual Analysis:

# NAVISH

## 1. Overview of My Role in MS2

For Milestone 2, my main responsibility was creating the data-cleaning stage of the project. The raw CSV files had many issues such as missing fields, extra spaces, inconsistent NOC and country names, and rows that didn’t match header lengths. My job was to clean and standardize these files so the rest of the pipeline could run smoothly.

When we ran the final version of the project, the console output showed that the cleaning stage worked correctly. For example, the system started with **155,861 athletes** and ended with **164,082** after Paris integration. The entire process completed in **11.92 seconds** in my first try, which confirmed that the cleaned data was stable and efficient for the later steps such as age calculation and Paris 2024 merging.

---

## 2. Assumptions and Decisions

### Row Length Consistency
I assumed that every row must match the number of columns in the header. Some rows were missing fields like birthdate or weight. Instead of removing them, I padded missing values with empty strings to keep the structure consistent. This prevents index errors later.

### Whitespace and Casing Normalization
Many entries had extra spaces or inconsistent capitalization, such as `" Republic of Korea"` or `"BULGARIA "`. I standardized these fields so dictionary lookups and merges would not fail due to formatting issues.

### Using NOC Codes as the Source of Truth
Country names vary a lot across historical files, but NOCs (e.g., USA, ITA, KOR) are consistent. I used NOCs as the authoritative identifiers during cleaning and validation.

### Basic Data Structures Only
Because the project does not allow external libraries like pandas, I used only core Python data structures: lists, dictionaries, and sets.

---

## 3. Data Structures I Used

### Lists
Used to store each CSV row. They naturally match the column layout and are easy to pad or trim.

### Dictionaries
Used for quick lookups, especially for mapping NOC → country and checking for duplicate codes. They help keep the merge logic clean and efficient.

### Sets
Used to detect duplicate NOCs and country entries. Fast membership checks make it simple to validate the data.

---

## 4. Cleaning Process

### Step 1 — Reading the Data
Each row is stripped of unnecessary whitespace and split into columns. This removes formatting issues early.

### Step 2 — Structural Validation
If a row has fewer columns than expected, I pad it with empty strings. If a row has more, I trim it. This guarantees stable indexing later in the program.

### Step 3 — Cleaning Key Fields
I fixed issues in NOC codes, country names, and other text fields by removing extra spaces and normalizing casing.

### Step 4 — Output Clean Data
The cleaned datasets become the inputs for date parsing, age calculations, Paris integration, and medal tally generation.

---

## 5. Why My Component Matters

The cleaning stage is the foundation for the entire project. Without consistent row structures and standardized fields, later steps would fail. For example:

- mismatched NOCs would cause countries to disappear from the medal tally  
- missing columns would break age calculations  
- inconsistent spacing would prevent proper merging of Paris athletes  

Because the cleaning stage fixes these issues before anything else runs, the integration phase works reliably. The final validation summary confirms this, showing:

- **164,082** athlete bio entries  
- **340,325** event result entries  
- **239** country entries  
- **64** games entries  
- **4,430** medal tally entries  

Overall, my work ensures that the entire pipeline starts with clean, predictable data, allowing the rest of the project to produce accurate results.

# GURJEET SINGH SODHI

## 1. Overview of My Role in MS2

For Milestone 2, my contributions focused on the documentation, AI tracking, integration support, and assisting in the Paris 2024 data merging implementation.  
In particular, I helped with debugging and improving the logic inside `integrate_paris_data()`, which handles:

- Adding Paris athletes and event results  
- Avoiding duplicates based on existing athletes  
- Maintaining consistent internal IDs

Along with that, I ensured:

- All AI/Web usage was recorded correctly in `prompts.md`  
- Repository workflow stayed consistent and error-free  
- Final validation of outputs before submission  

When we ran the final program, all five output files were created successfully, and the Paris data merged correctly without duplicating existing athletes.

---

## 2. Assumptions and Decisions

### Documentation Integrity Is Part of the Grade
Since AI usage influences academic honesty scoring, I ensured every prompt was logged with:

- Tool used  
- Prompt  
- Output summary  
- What was modified before applying  

This ensures we follow the milestone requirements accurately.

### Internal ID Rules Must Stay Consistent
The Paris dataset uses a different ID system.  
I supported implementation decisions in `integrate_paris_data()` to ensure:

- New athletes receive unique numeric IDs  
- Events reference the correct internal athlete IDs  

This prevents mismatches later in medal tally calculations.

### Team Events Require Name Normalization
During testing, we found that Paris team-event rows often:

- Contain multiple athletes in one column  
- Include spacing and punctuation differences  

I helped debug these cases and adjusted splitting and matching logic so team participants are correctly recognized and inserted.

### Ensuring Output Files Are External to Repo
To avoid losing marks due to pushing large data, I confirmed `.gitignore` behavior so `new_*.csv` files stay local only.

---

## 3. Integration & Validation Workflow

### Step 1 — Paris Insertion Verification
Checked that duplicate athletes were not added if they already existed in earlier Olympics.

### Step 2 — Field Formatting Validation
Confirmed consistent formats after cleaning:

- Birthdates use `dd-Mon-yyyy`  
- Games date ranges follow the same standard

### Step 3 — Age Calculation Rules
Verified that athletes with birthdays during an Olympics still show age as if the birthday had already passed.

### Step 4 — Medal Tally Accuracy
Cross-checked sample countries to ensure:

- gold + silver + bronze = total medals  
- `number_of_athletes` matches participation data  

These checks confirmed correctness of the final summary file.

---

### Final Performance Outcome
The program completed within the runtime limit, meeting the requirement of **less than 50 seconds** execution time.

---

## 4. Why My Component Matters

Even though most of the data transformation logic was completed by my teammates, my work ensured the final program:

- runs smoothly without missing data  
- integrates Paris results correctly  
- passes validation and generates all required files  
- meets academic honesty requirements through proper AI logging  

By helping inside `integrate_paris_data()`, I directly contributed to the correctness of Paris athlete merging and medal result alignment, which is a core requirement of Milestone 2.

Without the debugging and validation effort, mismatches in IDs or athlete names could break medal tallies or duplicate entries, impacting final grading.

---

### Final Statement

This milestone showed me that reliable data engineering is not only about writing code, but also:

- validating results carefully  
- ensuring documentation aligns with requirements  
- tracking integration workflows as teams collaborate  

I improved my ability to read unfamiliar datasets, resolve ID-matching issues, and maintain consistent output quality — skills that are essential in real-world software and data projects.

---

# MINHAZ

## 1. Overview of My Role in MS2

For Milestone 2, my main responsibilities were focused on the **time dimension and athlete lifecycle** in the dataset. Concretely, I worked on:

* Enhanced **date cleaning** for:

  * the `born` column (`clean_birth_date_enhanced`), and
  * the Olympic Games dates (`clean_single_games_date_enhanced`, `clean_games_date_enhanced`);
* Implementing the **age calculation logic** (`calculate_age`);
* Adding the **age column** to every event result (`add_age_to_events`);
* Co-developing the **Paris 2024 integration** logic, especially:

  * name normalization (`format_athlete_name`, `normalize_paris_name`),
  * merging Paris athletes and events into the main dataset (`integrate_paris_data`),
  * ensuring correct `isTeamSport` flags for Paris events.

Together, these components control when athletes were born, when games happened, how old athletes were at each event, and how Paris 2024 data is merged into the existing Olympic history. When the final program runs, my logic is responsible for:

* Standardizing dates across all files,
* Making sure **every event row** has a consistent, correctly computed age where possible,
* Ensuring Paris athletes and events align with the existing IDs and formats.

The final MS2 run (after optimizations) completes in about **5–6 seconds** and produces all required `new_*.csv` files, including `new_olympic_athlete_event_results.csv` with the age column and `new_medal_tally.csv` that depends on the correctness of event dates and Paris integration.

---

## 2. Assumptions and Decisions

### 2.1 Date Cleaning for Birth Dates

The raw `born` values in `olympic_athlete_bio.csv` were extremely inconsistent: full dates, partial dates, approximate years, and even noisy text. In `clean_birth_date_enhanced(date_str)` I made several explicit assumptions:

* **Standard output format**:
  All valid birth dates are normalized to `dd-Mon-yyyy` (e.g., `21-Oct-1991`). This single, strict format makes comparisons and age calculations much simpler.

* **Partial/approximate dates**:

  * If only a year is present (e.g., `"1884"`, `"c. 1884"`, `"(1884)"`), I assume the date is **01-Jan-<year>**.
    This is an approximation, but it preserves the year and allows age to be computed rather than discarded.
  * If the string contains noise but a valid date fragment can be extracted (e.g., `"born 21 October 1991"`), I extract and normalize it.

* **Paris ISO format**:
  Paris birth dates in `paris_athletes.csv` are in ISO format (`yyyy-mm-dd`). I treat those as valid and convert them directly into the same `dd-Mon-yyyy` format, so historical and Paris birth dates are fully aligned.

If the string cannot be parsed into any recognizable form, `clean_birth_date_enhanced` returns an empty string, signalling that age cannot be safely calculated.

### 2.2 Date Cleaning for Olympic Games

The Games dates are used as **reference event dates** for age computation. In `clean_single_games_date_enhanced` and `clean_games_date_enhanced`, my assumptions were:

* The **year** column in `olympics_games.csv` is the ground truth.
  Even if the original date text is messy, the year guides the normalization.
* All start, end, and competition dates are ultimately normalized to `dd-Mon-yyyy`.
  This allows direct string parsing for age computation later.
* **Paris 2024 special handling**:
  For any edition identified as the Paris 2024 Summer Olympics, I force:

  * start date = `26-Jul-2024`
  * end date   = `11-Aug-2024`

  regardless of how the raw dates appear. This ensures consistent age calculations and comparisons for Paris events, and guarantees that `clean_games_data` returns a reliable `paris_edition_id` used elsewhere.

### 2.3 Age Calculation Rules

In `calculate_age(birth_date, event_date)`, I had to choose a clear, consistent rule for how age is computed. The key decisions:

* Both `birth_date` and `event_date` are expected in `dd-Mon-yyyy` format. If either is invalid or missing, the function returns `""` (no age).

* Age is computed as integer years, using:

  * `age = event_year - birth_year`, and
  * subtracting 1 if the athlete has **not yet had their birthday** by the event date.

* When events span a **range** of dates (e.g., Games start and end), the effective `event_date` used in age calculation comes from `add_age_to_events` logic (competition date if available, otherwise start/end rules chosen there). The age function itself assumes it’s receiving an appropriate event date.

This ensures that an athlete whose birthday happens during the Games is treated as having that birthday by the time of the competition if the competition date is on or after their birthday.

### 2.4 Paris Integration and Team Sports

I co-implemented Paris integration with a focus on correctness and efficiency:

* **Name normalization**:

  * `format_athlete_name` ensures that names from different sources (main bio vs Paris) follow the same capitalization rules.
  * `normalize_paris_name` prefers `name_tv` when available (e.g., `"Artur ALEKSANYAN"`), then normalizes and, when possible, resolves reversed formats such as `"ALEKSANYAN Artur"` into `"Artur ALEKSANYAN"`.
* **Duplicate detection**:

  * Paris athletes are matched to existing athletes using `(normalized_name.lower(), NOC)` as a key.
  * This avoids re-adding athletes who already appear in previous Olympics.
* **Team sports detection**:

  * Instead of guessing from event names, we leverage `code_team` from `paris_medallists.csv`:

    * If an event has any medallist row with a non-empty `code_team`, that event is considered a **team event**.
    * These event names are stored in a set and then used to set `isTeamSport` for all corresponding Paris event rows.

This decision was critical to get realistic medal counts for Paris 2024 and to avoid overcounting team medals.

---

## 3. Data Structures I Used

Because the project is restricted to the Python standard library (no pandas, no NumPy in core logic), I structured my functions around:

### 3.1 Lists

* CSV data is represented as `List[List[str]]`.
* In `add_age_to_events`, I treat each row as a list, and append a computed `age` value as a new column.

### 3.2 Dictionaries

Key dictionaries I rely on:

1. **Birth date mapping** (populated by `clean_athlete_data`, used by my functions):

   ```python
   birth_dates[athlete_id] = cleaned_birth_date
   ```

   * Lookups are O(1), so computing ages for all events is O(n).

2. **Games date mapping** in `add_age_to_events`:

   ```python
   edition_dates[edition_id] = {
       "competition_date": ...,
       "start_date": ...,
       "end_date": ...
   }
   ```

   * This lets me obtain the effective event date for each `(edition_id)` in O(1) time.

3. **Paris integration maps** (co-designed but heavily used in my logic):

   * `existing_athletes[(normalized_name.lower(), NOC)] = athlete_id`
   * `id_to_bio_idx[athlete_id] = row_index`
   * `code_to_athlete_info[code_athlete] = {..., "athlete_id": ..., "events_str": ...}`

These dictionaries prevent repeated scanning of long lists and keep the overall complexity linear.

### 3.3 Sets

In my parts of the code, sets are particularly important in the medal tally and team integration pieces I worked on:

* `team_event_names = set()` – tracks events that are **definitely** team sports based on `code_team`.
* `seen_paris_team_medals = set()` – ensures we count each Paris team medal only once per country/event/medal type.
* `athletes` sets inside the medal tally – ensure that `number_of_athletes` per edition/NOC is based on **unique** athletes, not row counts.

These set operations are O(1) on average and contribute heavily to keeping the runtime down.

---

## 4. Processing Logic I Implemented

### 4.1 Date Cleaning Flow

1. `clean_birth_date_enhanced` is called from `clean_athlete_data` for every row in `olympic_athlete_bio.csv` and for new Paris athletes added during integration.
2. `clean_single_games_date_enhanced` and `clean_games_date_enhanced` are called from `clean_games_data` on the `start_date`, `end_date`, and/or `competition_date` fields in `olympics_games.csv`.

Together, these functions ensure that **all dates** used downstream (`birth_date`, `event_date`) follow a single format and are safe to feed into Python’s date parsing.

### 4.2 Age Column Addition

In `add_age_to_events`:

1. I precompute a mapping of `edition_id → event_date` from the cleaned games data, preferring `competition_date` when available, otherwise falling back to a start or end date.
2. For each event row in `olympic_athlete_event_results` (including Paris rows after integration):

   * Look up `athlete_id` → `birth_date` from `birth_dates`.
   * Look up `edition_id` → `event_date` from the games map.
   * Call `calculate_age(birth_date, event_date)` to produce an age string or `""`.
   * Append this age to the row.

The result is a new event results file where almost all rows have a consistent, meaningful age value.

### 4.3 Paris Integration Support

In `format_athlete_name`, `normalize_paris_name`, and `integrate_paris_data`, my contributions ensured that:

* Paris names are normalized in a way that **reduces duplicates** and better matches the main bio file.
* Paris athletes reuse existing IDs when possible and only create new IDs when necessary.
* Birth, height, weight, and country data from Paris are used to backfill missing values without overwriting existing ones.
* `isTeamSport` is set correctly for Paris events, which directly impacts the medal tally for 2024.

---

## 5. Runtime and Complexity Analysis (My Parts)

Using the assignment notation:

* **n** = number of records in `olympic_athlete_event_results.csv`
* **a** = number of records in `olympic_athlete_bio.csv`
* **p** = number of records in `paris_athletes.csv`
* **e** = number of records in `paris_events.csv`
* **m** = number of records in `paris_medallists.csv`

### 5.1 Date Cleaning for Born and Games

* `clean_birth_date_enhanced` is called once per athlete: **O(a + p)** total, since it’s used for both historical and Paris births.
* `clean_single_games_date_enhanced` and `clean_games_date_enhanced` are called once per games row (let’s call that g rows), so **O(g)**.

Each call does a constant amount of work (regex, parsing, simple conditionals), so the overall cost is **linear** in the size of the input files.

### 5.2 Age Calculation and Adding the Age Column

* `calculate_age` itself is **O(1)** per call.
* `add_age_to_events`:

  * Builds the `edition_dates` mapping in **O(g)**.
  * Iterates through all event rows (including Paris), which is about **n_total = n + n_paris**.
  * Each iteration does O(1) dictionary lookups and one call to `calculate_age`.

Overall complexity for my age component:

> **O(g + n_total)**
> which is effectively **O(n_total)** since g is small.

### 5.3 Paris Integration (Shared)

The Paris integration functions I co-implemented have complexity roughly:

* **O(a)** to build athlete maps,
* **O(e)** to build `event_to_sport`,
* **O(m)** to analyze medallists and discover team events,
* **O(p + P_ev)** to integrate all Paris athletes and their events (where `P_ev` is the total number of athlete-event combinations from Paris),
* plus smaller terms for team events and medal-only backfill.

Overall, my contributions stay within **linear time** in the size of each input, which is why the entire program still runs in under **10 seconds** (and currently around **5–6 seconds**) on GitHub Actions.

---

## 6. Why My Component Matters

My work sits at the intersection of **data correctness** and **temporal logic**:

* Without robust birth and games date cleaning:

  * Age calculations would fail or be wildly inconsistent.
  * Any analysis based on athlete age (e.g., distribution of medals by age) would be unreliable.
* Without a clear age calculation rule and the `add_age_to_events` pipeline:

  * `new_olympic_athlete_event_results.csv` would miss a key derived metric required for Milestone 2.
* Without careful name normalization and Paris integration support:

  * Paris athletes could be duplicated instead of merged with existing ones.
  * `isTeamSport` would be wrong for many events, badly distorting medal tallies for 2024.

Overall, my components ensure that every event in the final dataset has:

* A clean, consistent **event date**,
* A clean, consistent **athlete birth date** (where possible),
* A correctly computed **age** at the time of competition,
* And, for Paris 2024, correctly integrated athlete and event entries that align with the historical data model.

This combination directly affects both the **Cleaning** and **Paris** scores in the MS2 checker and underpins the accuracy of the final `new_medal_tally.csv` file.
