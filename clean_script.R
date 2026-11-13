# ==============================================================================
# Automatic Standalone ODK Data Cleaning Script
# Script: clean_script.R
# Purpose: Reproduce dashboard corrections offline on raw ODK Central datasets.
# Compatible with: RStudio (interactive file dialogs) & CLI (Rscript batch)
# ==============================================================================

# ------------------------------------------------------------------------------
# 1. Package Management & Auto-Installation
# ------------------------------------------------------------------------------
required_packages <- c("readr", "readxl", "writexl", "dplyr", "stringr", "purrr", "tibble")

missing_packages <- required_packages[!sapply(required_packages, requireNamespace, quietly = TRUE)]
if (length(missing_packages) > 0) {
  message("----------------------------------------------------------------------")
  message("Installing missing required package(s): ", paste(missing_packages, collapse = ", "))
  message("----------------------------------------------------------------------")
  install.packages(missing_packages, repos = "https://cloud.r-project.org")
}

suppressPackageStartupMessages({
  library(readr)
  library(readxl)
  library(writexl)
  library(dplyr)
  library(stringr)
  library(purrr)
  library(tibble)
})

message("\n================================================================================")
message("         BRICK KILN BASELINE DATA - OFFLINE CLEANING SCRIPT")
message("================================================================================")

# ------------------------------------------------------------------------------
# 2. Helper Functions
# ------------------------------------------------------------------------------

# Test if a value is NA, NULL, or an empty string
is_blank_or_na <- function(v) {
  if (is.null(v)) return(TRUE)
  if (length(v) == 0) return(TRUE)
  if (is.na(v[1])) return(TRUE)
  s <- trimws(as.character(v[1]))
  return(s == "" || s == "NA")
}

# Compare two values safely across data types
values_match <- function(v1, v2) {
  v1_empty <- is_blank_or_na(v1)
  v2_empty <- is_blank_or_na(v2)
  if (v1_empty && v2_empty) return(TRUE)
  if (v1_empty != v2_empty) return(FALSE)
  
  s1 <- trimws(as.character(v1))
  s2 <- trimws(as.character(v2))
  if (s1 == s2) return(TRUE)
  
  # Numeric equivalence check if both can parse as numeric (e.g. 25 vs 25.0)
  n1 <- suppressWarnings(as.numeric(s1))
  n2 <- suppressWarnings(as.numeric(s2))
  if (!is.na(n1) && !is.na(n2)) {
    return(n1 == n2)
  }
  
  return(FALSE)
}

# Safely cast string value to target column data type
safe_cast <- function(value_str, target_class) {
  if (is_blank_or_na(value_str)) {
    return(list(success = TRUE, value = NA, is_na = TRUE, error = NULL))
  }
  value_str <- trimws(as.character(value_str))
  
  if ("character" %in% target_class) {
    return(list(success = TRUE, value = as.character(value_str), is_na = FALSE, error = NULL))
  }
  
  if ("integer" %in% target_class) {
    num <- suppressWarnings(as.numeric(value_str))
    if (is.na(num)) {
      return(list(success = FALSE, value = NULL, is_na = FALSE, error = "Cannot convert non-numeric text to integer"))
    }
    int_val <- suppressWarnings(as.integer(num))
    return(list(success = TRUE, value = int_val, is_na = FALSE, error = NULL))
  }
  
  if ("numeric" %in% target_class || "double" %in% target_class) {
    num_val <- suppressWarnings(as.numeric(value_str))
    if (is.na(num_val)) {
      return(list(success = FALSE, value = NULL, is_na = FALSE, error = "Cannot convert text to numeric"))
    }
    return(list(success = TRUE, value = num_val, is_na = FALSE, error = NULL))
  }
  
  if ("logical" %in% target_class) {
    lowered <- tolower(value_str)
    if (lowered %in% c("true", "t", "1", "yes", "y")) {
      return(list(success = TRUE, value = TRUE, is_na = FALSE, error = NULL))
    } else if (lowered %in% c("false", "f", "0", "no", "n")) {
      return(list(success = TRUE, value = FALSE, is_na = FALSE, error = NULL))
    } else {
      return(list(success = FALSE, value = NULL, is_na = FALSE, error = "Cannot convert string to logical (TRUE/FALSE)"))
    }
  }
  
  if ("Date" %in% target_class) {
    d_val <- suppressWarnings(as.Date(value_str))
    if (is.na(d_val)) {
      return(list(success = FALSE, value = NULL, is_na = FALSE, error = "Cannot parse valid Date format"))
    }
    return(list(success = TRUE, value = d_val, is_na = FALSE, error = NULL))
  }
  
  if (any(c("POSIXct", "POSIXt") %in% target_class)) {
    dt_val <- suppressWarnings(as.POSIXct(value_str))
    if (is.na(dt_val)) {
      return(list(success = FALSE, value = NULL, is_na = FALSE, error = "Cannot parse valid POSIXct datetime format"))
    }
    return(list(success = TRUE, value = dt_val, is_na = FALSE, error = NULL))
  }
  
  # Default fallback: character
  return(list(success = TRUE, value = as.character(value_str), is_na = FALSE, error = NULL))
}

# Identify primary submission identifier column in dataset
find_id_col <- function(col_names) {
  candidates <- c("instanceID", "meta:instanceID", "meta-instanceID", "KEY", "instance_id")
  for (c in candidates) {
    if (c %in% col_names) return(c)
  }
  # Case-insensitive search
  for (c in candidates) {
    idx <- which(tolower(col_names) == tolower(c))
    if (length(idx) > 0) return(col_names[idx[1]])
  }
  return(NULL)
}

# Identify all paired (<variable>_old, <variable>_new) columns
find_old_new_pairs <- function(col_names) {
  old_cols <- col_names[grepl("_old$", col_names)]
  valid_vars <- character()
  for (oc in old_cols) {
    var_name <- sub("_old$", "", oc)
    nc <- paste0(var_name, "_new")
    if (nc %in% col_names) {
      valid_vars <- c(valid_vars, var_name)
    }
  }
  return(unique(valid_vars))
}

# ------------------------------------------------------------------------------
# 3. File Selection (Interactive RStudio Dialogs & CLI Fallback)
# ------------------------------------------------------------------------------
cli_args <- commandArgs(trailingOnly = TRUE)
orig_file <- NULL
corr_file <- NULL
out_dir   <- NULL

if (length(cli_args) >= 2) {
  orig_file <- cli_args[1]
  corr_file <- cli_args[2]
  if (length(cli_args) >= 3) out_dir <- cli_args[3]
} else if (interactive()) {
  cat("\nSTEP 1 OF 2: Please select your Original ODK Dataset (CSV or Excel)...\n")
  try({
    orig_file <- file.choose()
  }, silent = TRUE)
  
  if (!is.null(orig_file) && file.exists(orig_file)) {
    cat(sprintf("-> Selected Original Dataset: %s\n\n", orig_file))
    cat("STEP 2 OF 2: Please select your 'Old New Rows.csv' (Correction CSV)...\n")
    try({
      corr_file <- file.choose()
    }, silent = TRUE)
    if (!is.null(corr_file) && file.exists(corr_file)) {
      cat(sprintf("-> Selected Correction File: %s\n\n", corr_file))
    }
  }
} else {
  # Batch / Non-interactive fallback: look in working directory
  default_orig <- c("latest_odk_dataset.csv", "original_odk_dataset.csv", "brick_kiln_submissions.csv", "ODK_Original.xlsx")
  default_corr <- c("Old_New_Rows.csv", "Old New Rows.csv", "brick_kiln_audit_log.csv")
  
  for (f in default_orig) {
    if (file.exists(f)) { orig_file <- f; break }
  }
  for (f in default_corr) {
    if (file.exists(f)) { corr_file <- f; break }
  }
}

if (is.null(orig_file) || !file.exists(orig_file)) {
  stop("Original ODK dataset file not found or not selected. Please specify a valid file path or run in RStudio.")
}
if (is.null(corr_file) || !file.exists(corr_file)) {
  stop("Correction file ('Old New Rows.csv') not found or not selected. Please specify a valid file path or run in RStudio.")
}

if (is.null(out_dir) || out_dir == "" || out_dir == ".") {
  out_dir <- dirname(orig_file)
  if (is.null(out_dir) || out_dir == "" || out_dir == ".") {
    out_dir <- getwd()
  }
}
if (!dir.exists(out_dir)) {
  dir.create(out_dir, recursive = TRUE)
}

# ------------------------------------------------------------------------------
# 4. Load Datasets In-Memory (Original file is NEVER modified)
# ------------------------------------------------------------------------------
is_excel <- grepl("\\.xlsx?$|\\.xls$", tolower(orig_file))

message(sprintf("Loading Original ODK Dataset: '%s'...", orig_file))
if (is_excel) {
  df_orig <- readxl::read_excel(orig_file)
} else {
  df_orig <- readr::read_csv(orig_file, show_col_types = FALSE, guess_max = 50000)
}

# In-memory copy to receive approved updates
df_clean <- df_orig

message(sprintf("Loading Correction Records: '%s'...", corr_file))
df_corr <- readr::read_csv(corr_file, col_types = cols(.default = col_character()), show_col_types = FALSE)

# ------------------------------------------------------------------------------
# 5. Pre-flight Validation & Dynamic Variable Detection
# ------------------------------------------------------------------------------
orig_id_col <- find_id_col(names(df_orig))
if (is.null(orig_id_col)) {
  stop("Could not locate a submission ID column (e.g. 'instanceID', 'KEY') in the original ODK dataset.")
}

corr_id_col <- find_id_col(names(df_corr))
if (is.null(corr_id_col)) {
  stop("Could not locate a submission ID column (e.g. 'instanceID') in the correction CSV.")
}

var_pairs <- find_old_new_pairs(names(df_corr))
if (length(var_pairs) == 0) {
  stop("No valid variable pairs (<variable>_old and <variable>_new) found in the correction CSV.")
}

message(sprintf("-> Matched submission identifier column: '%s'", orig_id_col))
message(sprintf("-> Dynamically detected %d variable pair(s): %s", length(var_pairs), paste(var_pairs, collapse = ", ")))

# Check for duplicate IDs in correction CSV (Safeguard 4A)
raw_corr_ids <- as.character(df_corr[[corr_id_col]])
valid_mask <- !is.na(raw_corr_ids) & trimws(raw_corr_ids) != ""
duplicate_ids <- unique(raw_corr_ids[valid_mask][duplicated(raw_corr_ids[valid_mask])])
if (length(duplicate_ids) > 0) {
  message(sprintf("-> Notice: %d duplicate instanceID(s) detected in correction CSV. Ambiguous corrections for these IDs will be flagged and skipped.", length(duplicate_ids)))
}

# ------------------------------------------------------------------------------
# 6. Core Cleaning Pipeline & Safeguards Execution
# ------------------------------------------------------------------------------
cleaning_log <- list()

total_submissions <- nrow(df_orig)
total_correction_rows <- nrow(df_corr)
total_updated <- 0
total_unchanged <- 0
total_unmatched <- 0
total_mismatches <- 0
total_failed <- 0

# Track per-variable statistics for summary table
var_stats <- list()
for (v in var_pairs) {
  var_stats[[v]] <- list(updated = 0, unchanged = 0, mismatched = 0, failed = 0)
}

# Iterate through every row in the correction CSV
for (i in seq_len(nrow(df_corr))) {
  corr_row <- df_corr[i, ]
  inst_id <- trimws(as.character(corr_row[[corr_id_col]]))
  
  if (is.na(inst_id) || inst_id == "") {
    cleaning_log[[length(cleaning_log) + 1]] <- tibble(
      instanceID = paste0("ROW_", i),
      variable_name = "ALL",
      old_value = NA_character_,
      new_value = NA_character_,
      status = "MISSING_ID",
      details = "Correction row has missing or blank instanceID; skipped."
    )
    total_failed <- total_failed + 1
    next
  }
  
  # Safeguard: Skip ambiguous duplicate IDs
  if (inst_id %in% duplicate_ids) {
    cleaning_log[[length(cleaning_log) + 1]] <- tibble(
      instanceID = inst_id,
      variable_name = "ALL",
      old_value = NA_character_,
      new_value = NA_character_,
      status = "DUPLICATE_CORRECTION_ID",
      details = "Duplicate instanceID found in correction CSV; skipped to prevent ambiguous updates."
    )
    total_failed <- total_failed + 1
    next
  }
  
  # Match record in original ODK dataset by stable unique identifier
  match_indices <- which(as.character(df_clean[[orig_id_col]]) == inst_id)
  if (length(match_indices) == 0) {
    # Safeguard 4A: Submission ID not found in original dataset
    cleaning_log[[length(cleaning_log) + 1]] <- tibble(
      instanceID = inst_id,
      variable_name = "ALL",
      old_value = NA_character_,
      new_value = NA_character_,
      status = "UNMATCHED_SUBMISSION",
      details = sprintf("Submission ID '%s' does not exist in the original ODK dataset.", inst_id)
    )
    total_unmatched <- total_unmatched + 1
    next
  }
  
  target_idx <- match_indices[1]
  
  # Process each detected variable pair for this submission
  for (var_name in var_pairs) {
    old_col <- paste0(var_name, "_old")
    new_col <- paste0(var_name, "_new")
    
    val_old_raw <- corr_row[[old_col]]
    val_new_raw <- corr_row[[new_col]]
    
    # If both old and new are blank/NA, this variable was not edited in this row
    if (is_blank_or_na(val_old_raw) && is_blank_or_na(val_new_raw)) {
      next
    }
    
    old_val_clean <- if (is_blank_or_na(val_old_raw)) NA_character_ else as.character(val_old_raw)
    new_val_clean <- if (is_blank_or_na(val_new_raw)) NA_character_ else as.character(val_new_raw)
    old_disp <- if (is_blank_or_na(val_old_raw)) "<empty>" else as.character(val_old_raw)
    new_disp <- if (is_blank_or_na(val_new_raw)) "<empty>" else as.character(val_new_raw)
    
    # Check if variable exists in original dataset
    if (!var_name %in% names(df_clean)) {
      cleaning_log[[length(cleaning_log) + 1]] <- tibble(
        instanceID = inst_id,
        variable_name = var_name,
        old_value = old_val_clean,
        new_value = new_val_clean,
        status = "COLUMN_NOT_IN_ORIGINAL",
        details = sprintf("Variable '%s' does not exist in the original ODK dataset.", var_name)
      )
      total_failed <- total_failed + 1
      var_stats[[var_name]]$failed <- var_stats[[var_name]]$failed + 1
      next
    }
    
    # Rule: If old and new values are identical, do nothing
    if (values_match(val_old_raw, val_new_raw)) {
      cleaning_log[[length(cleaning_log) + 1]] <- tibble(
        instanceID = inst_id,
        variable_name = var_name,
        old_value = old_val_clean,
        new_value = new_val_clean,
        status = "UNCHANGED",
        details = "Old and new values are identical; no update needed."
      )
      total_unchanged <- total_unchanged + 1
      var_stats[[var_name]]$unchanged <- var_stats[[var_name]]$unchanged + 1
      next
    }
    
    # Safeguard 4B: Old value verification against current original ODK value
    odk_current_val <- df_clean[[var_name]][target_idx]
    odk_disp <- if (is_blank_or_na(odk_current_val)) "<empty>" else as.character(odk_current_val)
    
    if (!values_match(odk_current_val, val_old_raw)) {
      cleaning_log[[length(cleaning_log) + 1]] <- tibble(
        instanceID = inst_id,
        variable_name = var_name,
        old_value = old_val_clean,
        new_value = new_val_clean,
        status = "OLD_VALUE_MISMATCH",
        details = sprintf("Old value mismatch: correction file expected '%s', but original ODK dataset contains '%s'. Update skipped.",
                          old_disp, odk_disp)
      )
      total_mismatches <- total_mismatches + 1
      var_stats[[var_name]]$mismatched <- var_stats[[var_name]]$mismatched + 1
      next
    }
    
    # Safeguard 4D: Type safe conversion to match original column type
    target_class <- class(df_clean[[var_name]])
    # If the original column was empty (all NA logical), promote to target type
    if (all(is.na(df_clean[[var_name]]))) {
      if (!is.na(suppressWarnings(as.numeric(val_new_raw)))) {
        target_class <- "numeric"
        df_clean[[var_name]] <- as.numeric(df_clean[[var_name]])
      } else {
        target_class <- "character"
        df_clean[[var_name]] <- as.character(df_clean[[var_name]])
      }
    }
    
    cast_res <- safe_cast(val_new_raw, target_class)
    
    if (!cast_res$success) {
      cleaning_log[[length(cleaning_log) + 1]] <- tibble(
        instanceID = inst_id,
        variable_name = var_name,
        old_value = old_val_clean,
        new_value = new_val_clean,
        status = "TYPE_CONVERSION_ERROR",
        details = sprintf("Type conversion failed: %s (target type: %s)",
                          cast_res$error, paste(target_class, collapse = "/"))
      )
      total_failed <- total_failed + 1
      var_stats[[var_name]]$failed <- var_stats[[var_name]]$failed + 1
      next
    }
    
    # Apply the update in memory
    df_clean[[var_name]][target_idx] <- cast_res$value
    
    cleaning_log[[length(cleaning_log) + 1]] <- tibble(
      instanceID = inst_id,
      variable_name = var_name,
      old_value = old_val_clean,
      new_value = new_val_clean,
      status = "UPDATED",
      details = sprintf("Successfully updated value from '%s' to '%s'.",
                        old_disp, new_disp)
    )
    total_updated <- total_updated + 1
    var_stats[[var_name]]$updated <- var_stats[[var_name]]$updated + 1
  }
}

# ------------------------------------------------------------------------------
# 7. Dataset Integrity Verification
# ------------------------------------------------------------------------------
# Ensure no columns were added or deleted and order is strictly maintained
stopifnot(identical(names(df_clean), names(df_orig)))
stopifnot(nrow(df_clean) == nrow(df_orig))

if (length(cleaning_log) > 0) {
  df_log <- bind_rows(cleaning_log)
} else {
  df_log <- tibble(
    instanceID = character(),
    variable_name = character(),
    old_value = character(),
    new_value = character(),
    status = character(),
    details = character()
  )
}

# ------------------------------------------------------------------------------
# 8. Export Cleaned Dataset and Audit Artifacts
# ------------------------------------------------------------------------------
output_clean_csv <- file.path(out_dir, "Cleaned_ODK_Data.csv")
readr::write_csv(df_clean, output_clean_csv, na = "")
message(sprintf("-> Saved Cleaned Dataset (CSV): %s", output_clean_csv))

if (is_excel) {
  output_clean_xlsx <- file.path(out_dir, "Cleaned_ODK_Data.xlsx")
  writexl::write_xlsx(df_clean, output_clean_xlsx)
  message(sprintf("-> Saved Cleaned Dataset (Excel): %s", output_clean_xlsx))
}

output_log_csv <- file.path(out_dir, "Cleaning_Log.csv")
readr::write_csv(df_log, output_log_csv, na = "")
message(sprintf("-> Saved Cleaning Log: %s", output_log_csv))

# ------------------------------------------------------------------------------
# 9. Generate Cleaning_Summary.txt & Display Metrics
# ------------------------------------------------------------------------------
timestamp_str <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")

summary_lines <- c(
  "================================================================================",
  "               BRICK KILN DATA CLEANING PIPELINE - SUMMARY REPORT",
  "================================================================================",
  sprintf("Execution Date/Time        : %s", timestamp_str),
  sprintf("Original ODK Dataset       : %s", orig_file),
  sprintf("Correction File            : %s", corr_file),
  sprintf("Output Directory           : %s", out_dir),
  "--------------------------------------------------------------------------------",
  "CLEANING KPI METRICS:",
  "--------------------------------------------------------------------------------",
  sprintf("  Total Original Submissions : %d", total_submissions),
  sprintf("  Total Correction Records   : %d", total_correction_rows),
  sprintf("  Total Variables Processed  : %d", length(var_pairs)),
  sprintf("  Total Values Updated       : %d", total_updated),
  sprintf("  Total Unchanged Values     : %d", total_unchanged),
  sprintf("  Total Unmatched Submissions: %d", total_unmatched),
  sprintf("  Total Old-Value Mismatches : %d", total_mismatches),
  sprintf("  Total Failed Corrections   : %d", total_failed),
  "--------------------------------------------------------------------------------",
  "VARIABLE-BY-VARIABLE BREAKDOWN:",
  "--------------------------------------------------------------------------------",
  sprintf("%-35s %10s %10s %12s %10s", "Variable Name", "Updated", "Unchanged", "Mismatched", "Failed"),
  paste(rep("-", 80), collapse = "")
)

for (v in var_pairs) {
  st <- var_stats[[v]]
  summary_lines <- c(summary_lines, sprintf("%-35s %10d %10d %12d %10d", v, st$updated, st$unchanged, st$mismatched, st$failed))
}

summary_lines <- c(
  summary_lines,
  "================================================================================",
  "EXECUTION STATUS: COMPLETED SUCCESSFULLY",
  "================================================================================"
)

output_summary_txt <- file.path(out_dir, "Cleaning_Summary.txt")
writeLines(summary_lines, output_summary_txt)
message(sprintf("-> Saved Summary Report: %s", output_summary_txt))

# Display summary in console
cat("\n")
cat(paste(summary_lines, collapse = "\n"))
cat("\n\nAll cleaning operations completed successfully. Ready for analysis.\n")
