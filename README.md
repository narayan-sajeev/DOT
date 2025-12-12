# DOT Towing Company Verification

This project filters U.S. FMCSA DOT data to identify towing companies in New England and New York, then verifies each company using live web searches.

The result is a clean, verified list of real towing companies ready for analysis or outreach.

---

## What This Does

* Filters the FMCSA Company Census file to:

  * New England + New York only
  * Towing-related carriers
  * Companies above a minimum size threshold
* Verifies each company via web search
* Outputs a final CSV of verified towing companies

---

## Files

* **`dot.py`**
  Filters the raw FMCSA data and creates a Parquet file.

* **`Company_Census_File.parquet`**
  Filtered dataset for fast loading.

* **`processed_dots.csv`**
  Checkpoint file to track processed DOT numbers.

* **`towing_companies_verified.csv`**
  Final verified output.

---

## Notes

* Uses visible browsers to avoid search engine blocking.
* Designed to be restart-safe and reliable.
* Built for practical use, not academic demonstration.