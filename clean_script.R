# ==============================================================================
# Brick Kiln Data Quality Monitor - Standalone Cleaning & Validation Script
# Project: Brick Kiln Survey (Project 5)
# Form ID: brick_kiln_survey
# Generated for: RStudio & CLI Execution
# ==============================================================================

# Required packages
required_packages <- c("tidyverse", "lubridate", "readr", "stringr", "jsonlite")

for (pkg in required_packages) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    message(paste("Installing missing package:", pkg))
    install.packages(pkg, repos = "https://cloud.r-project.org")
  }
  library(pkg, character.only = TRUE)
}

message("----------------------------------------------------------------------")
message(" Brick Kiln Data Quality Monitor - Validation & Cleaning Engine")
message("----------------------------------------------------------------------")

# 1. Configuration & Input File
# Replace with your exported CSV file from the dashboard or ODK Central
input_file <- "brick_kiln_submissions.csv"

if (!file.exists(input_file)) {
  message(sprintf("File '%s' not found in current directory.", input_file))
  message("Generating sample dataframe for demonstration...")
  
  # Create synthetic demonstration dataset matching ODK Central Schema
  df_raw <- tribble(
    ~SubmissionDate, ~survey_date, ~interviewer, ~division, ~district, ~kiln_number, 
    ~kiln_unique_id, ~worker_name, ~worker_age, ~job_zone, ~worker_unique_id, 
    ~experience_years, ~daily_hours, ~ppe_used, ~resp_symptoms, ~heat_exhaustion, 
    ~drinking_water_access, ~medical_aid_access, ~monthly_income, ~`meta-instanceID`,
    "2026-09-20T06:30:38.355Z", "2026-09-20", "Yasin", 1, "06", "002", "106002", "Imran", 26, "firing", "106002_F", 5.0, 8, "gloves glasses", "sometimes", "no", "no", "yes", 5000, "uuid:58f4715f-b5b3-4cee-857d-909d1da12152",
    "2026-09-20T07:15:20.100Z", "2026-09-20", "Rahim", 2, "04", "015", "20405", "Rahim Ali", 34, "moulding", "20405_M", 4.0, 9, "none", "frequent", "no", "yes", "yes", 4800, "uuid:71a2bc44-8831-482f-889a-0012984bb101",
    "2026-09-20T08:00:10.500Z", "2026-09-20", "Yasin", 1, "06", "002", "106002", "Kabir Hossain", 38, "transport", "106002_T", 8.0, 10, "boots", "no", "no", "yes", "yes", 6200, "uuid:cc943211-4d33-4ee2-8233-22b4c9011133",
    "2026-09-21T09:20:00.000Z", "2026-09-21", "Tanvir", 3, "12", "008", "312008", "Salim Mia", 55, "loading", "312008_L", 15.0, 8, "gloves", "sometimes", "no", "yes", "yes", 5500, "uuid:dd054322-5e44-4dd3-9344-33c5d0122244",
    "2026-09-21T10:10:00.000Z", "2026-09-21", "Farhana", 2, "05", "010", "205010", "Jasim Uddin", 40, "firing", "205010_F", 7.0, 11, "mask", "frequent", "yes", "yes", "no", 5100, "uuid:ff276544-7a66-4bb5-1566-55e7f2344466",
    "2026-09-21T11:45:00.000Z", "2026-09-21", "Yasin", 1, "06", "003", "106003", "Abdul Karim", 29, "stacking", "106003_K", 3.0, 8, "gloves", "no", "no", "yes", "yes", 4500, "uuid:bb832100-3c22-4ff1-9122-11a3b8900022"
  )
} else {
  message(sprintf("Loading records from '%s'...", input_file))
  df_raw <- read_csv(input_file, col_types = cols(.default = "c"))
  df_raw$worker_age <- as.numeric(df_raw$worker_age)
  df_raw$experience_years <- as.numeric(df_raw$experience_years)
  df_raw$daily_hours <- as.numeric(df_raw$daily_hours)
  df_raw$monthly_income <- as.numeric(df_raw$monthly_income)
}

message(sprintf("Total submissions loaded: %d", nrow(df_raw)))

# 2. Validation & Business Logic Pipeline
# Rule 1: Kiln ID Reconstructed Check
# Expected: Division (1 digit) + District (2 digits with 0) + Kiln Number (3 digits with 0)
df_validated <- df_raw %>%
  mutate(
    # Clean string components
    div_clean = as.character(as.integer(division)),
    dist_clean = sprintf("%02d", as.integer(district)),
    kiln_clean = sprintf("%03d", as.integer(kiln_number)),
    expected_kiln_id = paste0(div_clean, dist_clean, kiln_clean),
    
    # Kiln ID Mismatch Flag
    flag_kiln_id_mismatch = (as.character(kiln_unique_id) != expected_kiln_id),
    
    # Expected Worker Unique ID: {kiln_unique_id}_{JobZoneInitial}
    job_zone_initial = toupper(substr(trimws(job_zone), 1, 1)),
    expected_worker_id = paste0(as.character(kiln_unique_id), "_", job_zone_initial),
    flag_worker_id_mismatch = (as.character(worker_unique_id) != expected_worker_id),
    
    # Human Verification Rules
    # Rule 2: Age boundary (<18 or >45)
    flag_age_boundary = (worker_age < 18 | worker_age > 45),
    
    # Rule 3: Health & Medical Aid Conflict
    flag_health_conflict = (tolower(heat_exhaustion) == "yes" & tolower(medical_aid_access) == "no"),
    
    # Rule 4: Warnings
    flag_extreme_hours = (daily_hours > 12)
  )

# Rule 5: Duplicate Kiln ID Check across dataset
kiln_id_counts <- df_validated %>%
  group_by(kiln_unique_id) %>%
  summarise(kiln_count = n(), .groups = "drop")

df_validated <- df_validated %>%
  left_join(kiln_id_counts, by = "kiln_unique_id") %>%
  mutate(flag_duplicate_kiln = (kiln_count > 1))

# 3. Compile Findings Table
findings_list <- list()

for (i in 1:nrow(df_validated)) {
  row <- df_validated[i, ]
  inst_id <- row$`meta-instanceID`
  
  if (isTRUE(row$flag_kiln_id_mismatch)) {
    findings_list[[length(findings_list) + 1]] <- data.frame(
      severity = "ERROR",
      instanceID = inst_id,
      field = "kiln_unique_id",
      original_value = as.character(row$kiln_unique_id),
      detected_issue = sprintf("Kiln ID Structure Mismatch (Reconstructed: %s, Found: %s)", row$expected_kiln_id, row$kiln_unique_id),
      stringsAsFactors = FALSE
    )
  }
  
  if (isTRUE(row$flag_worker_id_mismatch)) {
    findings_list[[length(findings_list) + 1]] <- data.frame(
      severity = "ERROR",
      instanceID = inst_id,
      field = "worker_unique_id",
      original_value = as.character(row$worker_unique_id),
      detected_issue = sprintf("Worker ID Structure Mismatch (Expected: %s, Found: %s)", row$expected_worker_id, row$worker_unique_id),
      stringsAsFactors = FALSE
    )
  }
  
  if (isTRUE(row$flag_duplicate_kiln)) {
    findings_list[[length(findings_list) + 1]] <- data.frame(
      severity = "DUPLICATE_KILN_ID",
      instanceID = inst_id,
      field = "kiln_unique_id",
      original_value = as.character(row$kiln_unique_id),
      detected_issue = sprintf("Duplicate Kiln ID (Appears %d times in dataset)", row$kiln_count),
      stringsAsFactors = FALSE
    )
  }
  
  if (isTRUE(row$flag_age_boundary)) {
    findings_list[[length(findings_list) + 1]] <- data.frame(
      severity = "REVIEW_FLAG",
      instanceID = inst_id,
      field = "worker_age",
      original_value = as.character(row$worker_age),
      detected_issue = sprintf("Worker Age Out of Range (<18 or >45: reported %s yrs)", row$worker_age),
      stringsAsFactors = FALSE
    )
  }
  
  if (isTRUE(row$flag_health_conflict)) {
    findings_list[[length(findings_list) + 1]] <- data.frame(
      severity = "REVIEW_FLAG",
      instanceID = inst_id,
      field = "medical_aid_access",
      original_value = sprintf("heat_exhaustion=%s, medical_aid=%s", row$heat_exhaustion, row$medical_aid_access),
      detected_issue = "Health & Medical Aid Conflict: Heat exhaustion reported without medical aid access",
      stringsAsFactors = FALSE
    )
  }
  
  if (isTRUE(row$flag_extreme_hours)) {
    findings_list[[length(findings_list) + 1]] <- data.frame(
      severity = "WARNING",
      instanceID = inst_id,
      field = "daily_hours",
      original_value = as.character(row$daily_hours),
      detected_issue = sprintf("Extreme daily working hours (>12 hrs: reported %s hrs)", row$daily_hours),
      stringsAsFactors = FALSE
    )
  }
}

if (length(findings_list) > 0) {
  findings_df <- bind_rows(findings_list)
} else {
  findings_df <- tibble(severity = character(), instanceID = character(), field = character(), original_value = character(), detected_issue = character())
}

# 4. KPI Metrics Summary
total_submissions <- nrow(df_validated)
errors_count <- sum(findings_df$severity == "ERROR")
duplicate_kiln_count <- sum(findings_df$severity == "DUPLICATE_KILN_ID")
review_flags_count <- sum(findings_df$severity == "REVIEW_FLAG")
warning_count <- sum(findings_df$severity == "WARNING")

# Valid records: records with no errors and no review flags
flagged_instance_ids <- unique(findings_df$instanceID[findings_df$severity %in% c("ERROR", "REVIEW_FLAG")])
valid_records_count <- total_submissions - length(flagged_instance_ids)

message("\n==================== DATA QUALITY KPI SUMMARY ====================")
message(sprintf("  TOTAL SUBMISSIONS   : %d", total_submissions))
message(sprintf("  VALID RECORDS       : %d", valid_records_count))
message(sprintf("  OPEN ERRORS         : %d", errors_count))
message(sprintf("  DUPLICATE KILN IDS  : %d", duplicate_kiln_count))
message(sprintf("  REVIEW FLAGS        : %d", review_flags_count))
message(sprintf("  WARNINGS            : %d", warning_count))
message("==================================================================\n")

# 5. Export Quality Reports & Clean Data
output_findings_csv <- "data_quality_findings_report.csv"
output_clean_csv <- "brick_kiln_clean_dataset.csv"

write_csv(findings_df, output_findings_csv)
message(sprintf("Findings report saved to: %s", output_findings_csv))

# Prepare clean dataset (retain original columns with standard types)
df_clean <- df_validated %>%
  select(all_of(names(df_raw)))

write_csv(df_clean, output_clean_csv)
message(sprintf("Cleaned dataset saved to: %s", output_clean_csv))
message("\nScript execution complete. Ready for analysis in RStudio.")
