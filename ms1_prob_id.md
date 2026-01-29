# Milestone 1 Problem Identification

### Problem Identification — Written by Navish

**1. What unknown or wrong data is there?**  
- Missing values in fields like `height`, `weight`, and `birth_date`.  
- Inconsistent country naming (e.g., “Russian Federation” vs “ROC”).  
- Some records missing athlete IDs or have duplicates.  

**2. How will wrong/unknown data be handled?**  
- Replace missing fields with `"Unknown"`.  
- Drop duplicate athlete entries based on `(name, country, birth_date)`.  
- Normalize country names using `country_noc` code.  

**3. How will Paris data be organized?**  
- Paris data (`paris/athletes.csv`) will mirror the structure of `olympic_athlete_bio.csv`.  
- Data will be grouped by `country` and linked by athlete ID.  

**4. How to determine duplicate athlete entries?**  
- Compare based on `name`, `birth_date`, and `country_noc`.  
- Use pandas `drop_duplicates()` method during data cleaning.  

**5. How will you know if your application works?**  
- 5 new output CSV files are generated successfully with proper headers.  
- The `age` column appears at the end of `new_olympic_athlete_event_results.csv`.  
- No missing headers or broken lines.  

**6. Specific records to check:**  
- `athlete_id: 65649` (Ivanka Bonova)  
- `athlete_id: 133041` (Vincent Riendeau)  
- A Paris record: `1532872` (Artur Aleksanyan)