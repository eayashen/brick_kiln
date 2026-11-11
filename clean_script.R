# ==============================================================================
# Brick Kiln Data Quality Monitor - Standalone Cleaning & Validation Script
# Form ID: baseline_2026_2027_ZZK2.0_Mymensingh_Narsingdi_Narayanganj (Project 7)
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
input_file <- if (file.exists("latest_odk_dataset.csv")) {
  "latest_odk_dataset.csv"
} else if (file.exists("brick_kiln_submissions.csv")) {
  "brick_kiln_submissions.csv"
} else {
  "latest_odk_dataset.csv"
}

if (!file.exists(input_file)) {
  stop(sprintf("Input file '%s' not found. Please export your clean or raw dataset from the dashboard or ODK Central.", input_file))
}

message(sprintf("Loading records from '%s'...", input_file))
df_raw <- read_csv(input_file, col_types = cols(.default = "c"))
message(sprintf("Total submissions loaded: %d", nrow(df_raw)))

# Detect Schema
is_baseline_2026 <- ("kiln_id" %in% names(df_raw) || "dist_geocode" %in% names(df_raw))

# 2. Validation & Business Logic Pipeline
df_validated <- df_raw

if (is_baseline_2026) {
  message("Executing Baseline 2026-2027 Survey Quality Rules...")
  
  df_validated <- df_validated %>%
    mutate(
      # Reconstructed 7-digit Kiln ID: dist_geocode (2d) + interviewer (2d) + id (3d)
      dist_clean = sprintf("%02d", as.integer(dist_geocode)),
      inv_clean = sprintf("%02d", as.integer(interviewer)),
      id_clean = sprintf("%03d", as.integer(id)),
      expected_kiln_id = paste0(dist_clean, inv_clean, id_clean),
      
      # Kiln ID Mismatch Flag (Only for consented submissions)
      flag_kiln_id_mismatch = (
        tolower(coalesce(consent, "")) != "no" &
        !is.na(expected_kiln_id) & 
        !is.na(kiln_id) & 
        as.character(kiln_id) != expected_kiln_id
      ),
      
      # Worker ID mismatch (Not applicable in facility baseline)
      flag_worker_id_mismatch = FALSE,
      
      # Owner Age boundary check (<18 or >70)
      owner_age_num = as.numeric(kiln_owner_age_years),
      flag_age_boundary = (!is.na(owner_age_num) & (owner_age_num < 18 | owner_age_num > 70)),
      
      # Rule 7: Daily Average Green Brick Production Capacity (<1000 or >100000)
      daily_prod_num = as.numeric(daily_green_brick_production),
      flag_daily_green_brick_production = (!is.na(daily_prod_num) & (daily_prod_num < 1000 | daily_prod_num > 100000)),
      
      flag_health_conflict = FALSE,
      flag_extreme_hours = FALSE
    )

  # Duplicate Kiln ID Check across dataset (ignoring consent == 'no')
  kiln_id_counts <- df_validated %>%
    filter(tolower(coalesce(consent, "")) != "no" & !is.na(kiln_id) & kiln_id != "") %>%
    group_by(kiln_id) %>%
    summarise(kiln_count = n(), .groups = "drop")

  df_validated <- df_validated %>%
    left_join(kiln_id_counts, by = "kiln_id") %>%
    mutate(
      kiln_count = coalesce(kiln_count, 0L),
      flag_duplicate_kiln = (tolower(coalesce(consent, "")) != "no" & kiln_count > 1)
    )

} else {
  message("Executing Legacy Worker Survey Quality Rules...")

  df_validated <- df_validated %>%
    mutate(
      worker_age_num = as.numeric(worker_age),
      daily_hours_num = as.numeric(daily_hours),
      div_clean = as.character(as.integer(division)),
      dist_clean = sprintf("%02d", as.integer(district)),
      kiln_clean = sprintf("%03d", as.integer(kiln_number)),
      expected_kiln_id = paste0(div_clean, dist_clean, kiln_clean),
      
      flag_kiln_id_mismatch = (as.character(kiln_unique_id) != expected_kiln_id),
      
      job_zone_initial = toupper(substr(trimws(coalesce(job_zone, "")), 1, 1)),
      expected_worker_id = paste0(as.character(kiln_unique_id), "_", job_zone_initial),
      flag_worker_id_mismatch = (as.character(worker_unique_id) != expected_worker_id),
      
      flag_age_boundary = (!is.na(worker_age_num) & (worker_age_num < 18 | worker_age_num > 45)),
      flag_health_conflict = (tolower(coalesce(heat_exhaustion, "")) == "yes" & tolower(coalesce(medical_aid_access, "")) == "no"),
      flag_extreme_hours = (!is.na(daily_hours_num) & daily_hours_num > 12)
    )

  kiln_id_counts <- df_validated %>%
    group_by(kiln_unique_id) %>%
    summarise(kiln_count = n(), .groups = "drop")

  df_validated <- df_validated %>%
    left_join(kiln_id_counts, by = "kiln_unique_id") %>%
    mutate(flag_duplicate_kiln = (kiln_count > 1))
}

# 3. Compile Findings Table
findings_list <- list()

for (i in seq_len(nrow(df_validated))) {
  row <- df_validated[i, ]
  inst_id <- if ("instanceID" %in% names(row) && !is.na(row[["instanceID"]])) {
    as.character(row[["instanceID"]])
  } else if ("meta-instanceID" %in% names(row) && !is.na(row[["meta-instanceID"]])) {
    as.character(row[["meta-instanceID"]])
  } else if ("KEY" %in% names(row) && !is.na(row[["KEY"]])) {
    as.character(row[["KEY"]])
  } else {
    paste0("row_", i)
  }
  
  if (isTRUE(row$flag_kiln_id_mismatch)) {
    actual_k_id <- if (is_baseline_2026) as.character(row$kiln_id) else as.character(row$kiln_unique_id)
    target_field <- if (is_baseline_2026) "kiln_id" else "kiln_unique_id"
    findings_list[[length(findings_list) + 1]] <- data.frame(
      severity = "ERROR",
      instanceID = inst_id,
      field = target_field,
      original_value = actual_k_id,
      detected_issue = sprintf("Kiln ID Formula Mismatch (Reconstructed: %s, Found: %s)", row$expected_kiln_id, actual_k_id),
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
    actual_k_id <- if (is_baseline_2026) as.character(row$kiln_id) else as.character(row$kiln_unique_id)
    target_field <- if (is_baseline_2026) "kiln_id" else "kiln_unique_id"
    findings_list[[length(findings_list) + 1]] <- data.frame(
      severity = "DUPLICATE_KILN_ID",
      instanceID = inst_id,
      field = target_field,
      original_value = actual_k_id,
      detected_issue = sprintf("Duplicate Kiln ID (Appears %d times in dataset)", row$kiln_count),
      stringsAsFactors = FALSE
    )
  }
  
  if (isTRUE(row$flag_age_boundary)) {
    if (is_baseline_2026) {
      findings_list[[length(findings_list) + 1]] <- data.frame(
        severity = "REVIEW_FLAG",
        instanceID = inst_id,
        field = "kiln_owner_age_years",
        original_value = as.character(row$kiln_owner_age_years),
        detected_issue = sprintf("Kiln Owner Age Out of Standard Range (<18 or >70: reported %s yrs)", row$kiln_owner_age_years),
        stringsAsFactors = FALSE
      )
    } else {
      findings_list[[length(findings_list) + 1]] <- data.frame(
        severity = "REVIEW_FLAG",
        instanceID = inst_id,
        field = "worker_age",
        original_value = as.character(row$worker_age),
        detected_issue = sprintf("Worker Age Out of Range (<18 or >45: reported %s yrs)", row$worker_age),
        stringsAsFactors = FALSE
      )
    }
  }
  
  if (isTRUE(row$flag_daily_green_brick_production)) {
    prod_val <- as.numeric(row$daily_green_brick_production)
    issue_msg <- if (prod_val < 1000) {
      sprintf("Daily Average Green Brick Production Capacity Below Standard Range (<1000: reported %s bricks/day)", as.character(row$daily_green_brick_production))
    } else {
      sprintf("Daily Average Green Brick Production Capacity Above Standard Range (>100000: reported %s bricks/day)", as.character(row$daily_green_brick_production))
    }
    findings_list[[length(findings_list) + 1]] <- data.frame(
      severity = "REVIEW_FLAG",
      instanceID = inst_id,
      field = "daily_green_brick_production",
      original_value = as.character(row$daily_green_brick_production),
      detected_issue = issue_msg,
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