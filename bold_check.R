#!/usr/bin/env Rscript
# bold_check.R
#
# Queries BOLD via BOLDconnectR using specimen IDs from the portal dump.
# Compares BOLD records against portal data to find:
#   1. Specimens on BOLD but missing from portal
#   2. Specimens in portal marked as uploaded but not found on BOLD
#   3. Specimens with sequences but no BIN URI (bin_uri is NA)
#   4. BIN URI status at plate level
#
# Requires: module load HGI/softpack/users/aw43/BOLDconnectR_bioscan/2
# API key : set BOLD_API_KEY environment variable (never hardcode)
#
# Usage:
#   export BOLD_API_KEY="your-key-here"
#   Rscript bold_check.R
#   Rscript bold_check.R --partner BGEP
#   Rscript bold_check.R --batch-size 500
#
# Output: results/bold_check_YYYYMMDD.csv
#         results/bold_check_plate_summary_YYYYMMDD.csv

suppressPackageStartupMessages({
  library(BOLDconnectR)
  library(dplyr)
  library(readr)
})

# ── Parse arguments ───────────────────────────────────────────────────────────
args <- commandArgs(trailingOnly = TRUE)
partner    <- if ("--partner" %in% args) args[which(args == "--partner") + 1] else "ALL"
batch_size <- if ("--batch-size" %in% args) as.integer(args[which(args == "--batch-size") + 1]) else 500

# ── Config ────────────────────────────────────────────────────────────────────
PORTAL_DUMP <- "/lustre/scratch126/tol/teams/lawniczak/projects/bioscan/100k_paper/output/sts_manifests_20260408.tsv"
RESULTS_DIR <- "/lustre/scratch126/tol/teams/lawniczak/users/lp20/bioscan_plate_checker_results"
today       <- format(Sys.Date(), "%Y%m%d")

dir.create(RESULTS_DIR, showWarnings = FALSE, recursive = TRUE)

# ── API key ───────────────────────────────────────────────────────────────────
api_key <- Sys.getenv("BOLD_API_KEY")
if (nchar(api_key) == 0) {
  stop("BOLD_API_KEY environment variable not set.\n",
       "Run: export BOLD_API_KEY='your-key-here'")
}
bold.apikey(api_key)
cat("API key loaded.\n")

# ── Load portal dump ──────────────────────────────────────────────────────────
cat("Reading portal dump...\n")
portal_df <- read_tsv(PORTAL_DUMP, col_types = cols(.default = "c"),
                      show_col_types = FALSE)

# Extract plate ID from specimen ID (everything before last underscore)
portal_df <- portal_df %>%
  mutate(
    specimen_id = `sts_specimen.id`,
    plate_id    = sub("_[^_]+$", "", specimen_id),
    bold_nuc_present = !is.na(bold_nuc) & bold_nuc != "None" & nchar(bold_nuc) > 10
  )

# Filter to specimens marked as uploaded to BOLD
bold_uploaded <- portal_df %>%
  filter(bold_nuc_present)

cat(sprintf("Portal dump: %d total specimens, %d marked as BOLD uploaded\n",
            nrow(portal_df), nrow(bold_uploaded)))

# Filter by partner if specified
if (partner != "ALL") {
  bold_uploaded <- bold_uploaded %>%
    filter(grepl(paste0("^", partner, "[-_]|^TOL-", partner, "-"),
                 plate_id, ignore.case = FALSE))
  cat(sprintf("Filtered to partner '%s': %d specimens\n", partner, nrow(bold_uploaded)))
}

if (nrow(bold_uploaded) == 0) {
  cat("No specimens to query.\n")
  quit(status = 0)
}

# ── Query BOLD in batches ─────────────────────────────────────────────────────
specimen_ids <- bold_uploaded$specimen_id
cat(sprintf("Querying BOLD for %d specimens in batches of %d...\n",
            length(specimen_ids), batch_size))

all_results <- list()
n_batches   <- ceiling(length(specimen_ids) / batch_size)

for (i in seq_len(n_batches)) {
  start_idx <- (i - 1) * batch_size + 1
  end_idx   <- min(i * batch_size, length(specimen_ids))
  batch_ids <- specimen_ids[start_idx:end_idx]

  cat(sprintf("  Batch %d/%d (specimens %d-%d)...\n",
              i, n_batches, start_idx, end_idx))

  result <- tryCatch({
    bold.fetch(get_by = "sampleid", identifiers = batch_ids)
  }, error = function(e) {
    cat(sprintf("  ERROR in batch %d: %s\n", i, e$message))
    NULL
  })

  if (!is.null(result) && nrow(result) > 0) {
    all_results[[i]] <- result
  }

  if (i < n_batches) Sys.sleep(1)
}

# ── Combine results ───────────────────────────────────────────────────────────
if (length(all_results) == 0) {
  cat("No results returned from BOLD.\n")
  quit(status = 1)
}

bold_df <- bind_rows(all_results)
cat(sprintf("BOLD returned %d records for %d unique specimen IDs\n",
            nrow(bold_df), n_distinct(bold_df$sampleid)))

# Extract plate ID from sampleid
bold_df <- bold_df %>%
  mutate(plate_id = sub("_[^_]+$", "", sampleid))

# ── Specimen-level comparison ─────────────────────────────────────────────────
# Join portal vs BOLD
comparison <- bold_uploaded %>%
  select(specimen_id, plate_id, bold_nuc_present, sts_submit_date) %>%
  full_join(
    bold_df %>% select(sampleid, nuc, bin_uri, sequence_upload_date,
                       plate_id) %>%
      rename(specimen_id = sampleid, bold_plate_id = plate_id),
    by = "specimen_id"
  ) %>%
  mutate(
    in_portal     = !is.na(bold_nuc_present),
    in_bold       = !is.na(nuc) & nchar(nuc) > 10,
    has_bin       = !is.na(bin_uri) & bin_uri != "None",
    status = case_when(
      in_portal & in_bold  ~ "FOUND_BOTH",
      in_portal & !in_bold ~ "PORTAL_ONLY",   # marked uploaded but not on BOLD
      !in_portal & in_bold ~ "BOLD_ONLY",     # on BOLD but not in portal dump
      TRUE                 ~ "UNKNOWN"
    )
  )

# ── Plate-level summary ───────────────────────────────────────────────────────
plate_summary <- comparison %>%
  group_by(plate_id) %>%
  summarise(
    n_specimens      = n(),
    n_found_both     = sum(status == "FOUND_BOTH"),
    n_portal_only    = sum(status == "PORTAL_ONLY"),
    n_bold_only      = sum(status == "BOLD_ONLY"),
    n_with_bin       = sum(has_bin, na.rm = TRUE),
    n_without_bin    = sum(in_bold & !has_bin, na.rm = TRUE),
    pct_on_bold      = round(100 * n_found_both / n_specimens, 1),
    earliest_upload  = min(sequence_upload_date, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(plate_id)

# ── Print summary ─────────────────────────────────────────────────────────────
cat("\n=== BOLD CHECK SUMMARY ===\n")
cat(sprintf("Specimens queried         : %d\n", nrow(bold_uploaded)))
cat(sprintf("Found on BOLD             : %d\n", sum(comparison$status == "FOUND_BOTH")))
cat(sprintf("Portal-only (not on BOLD) : %d\n", sum(comparison$status == "PORTAL_ONLY")))
cat(sprintf("BOLD-only (not in portal) : %d\n", sum(comparison$status == "BOLD_ONLY")))
cat(sprintf("With sequence, no BIN URI : %d\n",
            sum(comparison$in_bold & !comparison$has_bin, na.rm = TRUE)))
cat(sprintf("\nPlates with missing BINs  : %d\n",
            sum(plate_summary$n_without_bin > 0)))

# ── Save outputs ──────────────────────────────────────────────────────────────
specimen_out <- file.path(RESULTS_DIR, paste0("bold_check_specimens_", today, ".csv"))
plate_out    <- file.path(RESULTS_DIR, paste0("bold_check_plates_", today, ".csv"))

write_csv(comparison, specimen_out)
write_csv(plate_summary, plate_out)

cat(sprintf("\nOutputs written:\n  %s\n  %s\n", specimen_out, plate_out))
