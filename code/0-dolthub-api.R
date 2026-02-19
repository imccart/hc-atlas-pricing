# Meta --------------------------------------------------------------------

## Title:         Dolthub API Extraction
## Author:        Ian McCarthy
## Date Created:  2026-02-19
## Date Edited:   2026-02-19
## Description:   Downloads hospital and rate data from the Dolthub SQL API
##                for dolthub/transparency-in-pricing.
##
##                The rate table (~794M rows) has a composite PK on
##                (hospital_id, row_id) with no secondary indexes. Per-code
##                queries (WHERE ms_drg = X) force full-table scans and timeout.
##                Instead, we iterate per-hospital: each hospital's rows are
##                contiguous in the PK, so per-hospital queries are fast.
##
## Prerequisites: Run 0-setup.R and code-lists.R first (loaded by _build.R).

# Config ------------------------------------------------------------------

base_url  <- "https://www.dolthub.com/api/v1alpha1/dolthub/transparency-in-pricing/main"
out_dir   <- "data/input/dolthub"
hosp_dir  <- file.path(out_dir, "rates-by-hospital")
page_size <- 1000
sleep_sec <- 0.3          # sleep between API calls (seconds)
sleep_empty <- 0.05       # shorter sleep when hospital returns 0 rows

dir.create(hosp_dir, recursive = TRUE, showWarnings = FALSE)

# Helper: query Dolthub API -----------------------------------------------

query_dolthub <- function(sql, timeout_sec = 120) {
  resp <- request(base_url) |>
    req_url_query(q = sql) |>
    req_retry(max_tries = 5, backoff = ~ 2) |>
    req_timeout(timeout_sec) |>
    req_perform()

  body <- resp_body_json(resp)

  if (length(body$rows) == 0) return(tibble())

  cols <- names(body$rows[[1]])
  map_dfr(body$rows, function(row) {
    vals <- map_chr(row, ~ if (is.null(.x)) NA_character_ else as.character(.x))
    names(vals) <- cols
    as_tibble_row(vals)
  })
}

# Helper: paginated query -------------------------------------------------

paginated_query <- function(select_clause, from_table, where_clause = NULL,
                            key_col = "row_id", page_size = 1000) {
  all_rows <- list()
  last_key  <- NULL
  page      <- 0

  repeat {
    page <- page + 1

    where_parts <- c()
    if (!is.null(where_clause)) where_parts <- c(where_parts, where_clause)
    if (!is.null(last_key)) {
      where_parts <- c(where_parts, paste0(key_col, " > '", last_key, "'"))
    }

    where_sql <- ""
    if (length(where_parts) > 0) {
      where_sql <- paste0(" WHERE ", paste(where_parts, collapse = " AND "))
    }

    sql <- paste0("SELECT ", select_clause,
                  " FROM `", from_table, "`",
                  where_sql,
                  " ORDER BY ", key_col,
                  " LIMIT ", page_size)

    chunk <- query_dolthub(sql)
    if (nrow(chunk) == 0) break

    all_rows[[page]] <- chunk
    last_key <- chunk[[key_col]][nrow(chunk)]

    if (page %% 10 == 0) {
      message("    Page ", page, " (", sum(map_int(all_rows, nrow)), " rows so far)")
    }

    if (nrow(chunk) < page_size) break
    Sys.sleep(sleep_sec)
  }

  if (length(all_rows) == 0) return(tibble())
  bind_rows(all_rows)
}

# =========================================================================
# 1. Hospital table (full export, ~12K rows)
# =========================================================================

hosp_file <- file.path(out_dir, "hospital.csv")

if (file.exists(hosp_file)) {
  message("Hospital file already exists, skipping: ", hosp_file)
} else {
  message("Downloading hospital table...")
  hospitals <- paginated_query(
    select_clause = "*",
    from_table    = "hospital",
    key_col       = "id",
    page_size     = page_size
  )
  message("  Downloaded ", nrow(hospitals), " hospital rows")
  write_csv(hospitals, hosp_file)
  message("  Wrote ", hosp_file)
}

# =========================================================================
# 2. Rates: per-hospital queries for target DRG/CPT codes
# =========================================================================

drg_combined_file <- file.path(out_dir, "rates-drg.csv")
cpt_combined_file <- file.path(out_dir, "rates-cpt.csv")

if (file.exists(drg_combined_file) && file.exists(cpt_combined_file)) {
  message("Rate files already exist, skipping extraction.")
} else {

  # -- Build code filter clause -------------------------------------------

  drg_in <- paste0("'", target_drgs$code, "'", collapse = ",")
  cpt_in <- paste0("'", target_cpts$code, "'", collapse = ",")
  code_filter <- paste0("(ms_drg IN (", drg_in, ") OR hcpcs_cpt IN (", cpt_in, "))")

  # -- Load hospital IDs --------------------------------------------------

  hosp_data <- read_csv(hosp_file, col_types = cols(.default = "c"))
  hosp_ids  <- sort(unique(hosp_data$id))
  message("Loaded ", length(hosp_ids), " hospital IDs")

  # -- Track progress (resumability) --------------------------------------
  # Progress file: one hospital_id per line = already processed
  progress_file <- file.path(out_dir, "rates-progress.txt")
  done_ids <- character(0)
  if (file.exists(progress_file)) {
    done_ids <- readLines(progress_file)
    message("Resuming: ", length(done_ids), " hospitals already processed")
  }
  remaining <- setdiff(hosp_ids, done_ids)
  message("Hospitals to process: ", length(remaining))

  # -- Query each hospital ------------------------------------------------

  total_rows   <- 0
  hosp_with_data <- 0
  errors       <- 0
  t_start      <- Sys.time()

  for (idx in seq_along(remaining)) {
    hid <- remaining[idx]

    # Progress logging every 100 hospitals
    if (idx %% 100 == 0 || idx == 1) {
      elapsed <- as.numeric(difftime(Sys.time(), t_start, units = "mins"))
      rate_per_min <- if (elapsed > 0) round(idx / elapsed) else NA
      eta_min <- if (!is.na(rate_per_min) && rate_per_min > 0) {
        round((length(remaining) - idx) / rate_per_min)
      } else NA
      message(sprintf("[%s] Hospital %d/%d (%s) | %d rows from %d hospitals | %.1f min elapsed | ETA ~%s min",
        format(Sys.time(), "%H:%M:%S"), idx, length(remaining), hid,
        total_rows, hosp_with_data, elapsed,
        if (is.na(eta_min)) "?" else as.character(eta_min)))
    }

    # Per-hospital file for resumability
    part_file <- file.path(hosp_dir, paste0(hid, ".csv"))

    if (file.exists(part_file)) {
      # Already downloaded in a previous partial run (but not yet in progress file)
      n <- nrow(fread(part_file, nrows = 0))
      total_rows <- total_rows + n
      hosp_with_data <- hosp_with_data + 1
      cat(hid, "\n", file = progress_file, append = TRUE)
      next
    }

    # Query this hospital for all target codes (paginated)
    tryCatch({
      hosp_rates <- paginated_query(
        select_clause = "*",
        from_table    = "rate",
        where_clause  = paste0("hospital_id = '", hid, "' AND ", code_filter),
        key_col       = "row_id",
        page_size     = page_size
      )

      if (nrow(hosp_rates) > 0) {
        write_csv(hosp_rates, part_file)
        total_rows <- total_rows + nrow(hosp_rates)
        hosp_with_data <- hosp_with_data + 1
      }

      cat(hid, "\n", file = progress_file, append = TRUE)
      Sys.sleep(if (nrow(hosp_rates) == 0) sleep_empty else sleep_sec)

    }, error = function(e) {
      errors <<- errors + 1
      message("  ERROR on ", hid, ": ", conditionMessage(e))
      # Don't record in progress file — will retry on next run
      Sys.sleep(2)
    })
  }

  message(sprintf("\nPer-hospital extraction complete: %d rows from %d hospitals (%d errors)",
    total_rows, hosp_with_data, errors))

  # -- Combine per-hospital files into DRG/CPT CSVs -----------------------

  message("Combining per-hospital files...")
  part_files <- list.files(hosp_dir, pattern = "\\.csv$", full.names = TRUE)
  message("  Found ", length(part_files), " hospital files")

  if (length(part_files) == 0) {
    message("  WARNING: No rate data downloaded")
  } else {
    all_rates <- map_dfr(part_files, function(f) {
      fread(f, colClasses = "character") |> as_tibble()
    })
    message("  Total rows: ", nrow(all_rates))

    # Split into DRG and CPT based on which code column is populated
    drg_rates <- all_rates |> filter(!is.na(ms_drg) & ms_drg != "")
    cpt_rates <- all_rates |> filter(!is.na(hcpcs_cpt) & hcpcs_cpt != "")

    message("  DRG rows: ", nrow(drg_rates))
    message("  CPT rows: ", nrow(cpt_rates))

    if (nrow(drg_rates) > 0) {
      write_csv(drg_rates, drg_combined_file)
      message("  Wrote ", drg_combined_file)
    } else {
      message("  WARNING: No DRG rows")
    }

    if (nrow(cpt_rates) > 0) {
      write_csv(cpt_rates, cpt_combined_file)
      message("  Wrote ", cpt_combined_file)
    } else {
      message("  WARNING: No CPT rows")
    }
  }
}

message("\nDolthub API extraction complete.")
message("Output directory: ", out_dir)
