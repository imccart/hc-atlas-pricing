# Meta --------------------------------------------------------------------

## Title:         Oria Data Extraction
## Author:        Ian McCarthy
## Date Created:  2026-02-19
## Date Edited:   2026-02-19
## Description:   Extracts hospital metadata and rate data from the Trilliant
##                Health Oria consolidated data (DuckDB + parquet files).
##                Queries parquet files directly for target billing codes,
##                then outputs CSVs in the format expected by downstream scripts.
##
## Prerequisites: Run 0-setup.R and code-lists.R first (loaded by _build.R).
##                Unzip mrf_lake.zip in the raw data dir so mrf_lake.duckdb
##                and parquet/ sit side by side.

# Config ------------------------------------------------------------------

raw_dir     <- "data/input/oria"        # symlink to research-data/
oria_db     <- file.path(raw_dir, "mrf_lake.duckdb")
parquet_dir <- file.path(raw_dir, "parquet")
out_dir     <- "data/output"

dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

# Verify data exists ------------------------------------------------------

if (!dir.exists(parquet_dir)) {
  stop("Parquet directory not found: ", parquet_dir,
       "\nSymlink data/input/oria to the Oria data directory.")
}

# =========================================================================
# 1. Hospital metadata (from embedded DuckDB tables)
# =========================================================================

hosp_file <- file.path(out_dir, "oria-hospital.csv")

if (file.exists(hosp_file)) {
  message("Hospital file already exists, skipping: ", hosp_file)
} else {
  message("Extracting hospital metadata...")

  con_meta <- dbConnect(duckdb(), dbdir = oria_db, read_only = TRUE)

  # Hospitals table
  hosp <- dbGetQuery(con_meta, "
    SELECT hospital_id, hospital_name, hospital_address, hospital_city,
           hospital_state, total_charges_count, status
    FROM hospitals
    WHERE status = 'completed'
  ")

  # EIN mapping from mrf_metadata filenames
  mrf <- dbGetQuery(con_meta, "
    SELECT hospital_id, filename FROM mrf_metadata WHERE filename IS NOT NULL
  ")
  dbDisconnect(con_meta, shutdown = TRUE)

  hosp <- as_tibble(hosp)
  mrf  <- as_tibble(mrf)

  # Extract EIN from filename (first 9 digits)
  mrf <- mrf %>%
    mutate(ein = str_extract(filename, "^\\d{9}")) %>%
    filter(!is.na(ein)) %>%
    select(hospital_id, ein) %>%
    distinct(hospital_id, .keep_all = TRUE)

  hosp <- hosp %>%
    left_join(mrf, by = "hospital_id")

  message("  ", nrow(hosp), " hospitals with data (status = completed)")
  message("  EIN extracted: ", sum(!is.na(hosp$ein)), " / ", nrow(hosp))
  message("  Columns: ", paste(names(hosp), collapse = ", "))

  write_csv(hosp, hosp_file)
  message("  Wrote ", hosp_file)
}

# =========================================================================
# 2. Rate data for target DRGs and CPTs
# =========================================================================

drg_file <- file.path(out_dir, "oria-rates-drg.csv")
cpt_file <- file.path(out_dir, "oria-rates-cpt.csv")

if (file.exists(drg_file) && file.exists(cpt_file)) {
  message("\nRate files already exist, skipping extraction.")
} else {

  # Use in-memory DuckDB with parquet reads
  con <- dbConnect(duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Discover available states (parquet Hive partitions)
  state_dirs <- list.dirs(
    file.path(parquet_dir, "standard_charge_details"),
    recursive = FALSE, full.names = FALSE
  )
  states <- str_extract(state_dirs, "(?<=hospital_state=).+")
  states <- states[!is.na(states)]
  message("\nAvailable states: ", length(states))

  # -- Helper: query one state's parquet files for target codes ---------------
  query_state <- function(state, code_filter) {
    parquet_path <- file.path(
      parquet_dir,
      "standard_charge_details",
      paste0("hospital_state=", state),
      "*.parquet"
    )

    # Normalize path separators for DuckDB
    parquet_path <- gsub("\\\\", "/", parquet_path)

    sql <- paste0(
      "SELECT hospital_id, description, ms_drg, cpt, hcpcs, ",
      "payer_name, plan_name, ",
      "standard_charge_dollar, gross_charge, discounted_cash, minimum, maximum, ",
      "methodology, setting, billing_class, ",
      "hospital_name, hospital_state ",
      "FROM read_parquet('", parquet_path, "') ",
      "WHERE ", code_filter
    )

    tryCatch(
      dbGetQuery(con, sql),
      error = function(e) {
        message("    ERROR in state ", state, ": ", conditionMessage(e))
        NULL
      }
    )
  }

  # -- Build code filters ---------------------------------------------------
  drg_in <- paste0("'", target_drgs$code, "'", collapse = ", ")
  cpt_in <- paste0("'", target_cpts$code, "'", collapse = ", ")

  drg_filter <- paste0("ms_drg IN (", drg_in, ")")
  cpt_filter <- paste0("(cpt IN (", cpt_in, ") OR hcpcs IN (", cpt_in, "))")

  # -- Extract state by state -----------------------------------------------

  extract_by_state <- function(code_filter, label) {
    message("\n  Extracting ", label, " across ", length(states), " states...")
    all_parts <- list()
    total_rows <- 0
    errors <- 0

    for (i in seq_along(states)) {
      st <- states[i]
      if (i %% 10 == 0 || i == 1) {
        message("    State ", i, "/", length(states), " (", st, ")")
      }

      result <- query_state(st, code_filter)
      if (is.null(result)) {
        errors <- errors + 1
        next
      }
      if (nrow(result) > 0) {
        all_parts[[length(all_parts) + 1]] <- as_tibble(result)
        total_rows <- total_rows + nrow(result)
      }
    }

    message("    Done: ", format(total_rows, big.mark = ","), " rows from ",
            length(all_parts), " states (", errors, " errors)")

    if (length(all_parts) == 0) return(tibble())
    bind_rows(all_parts)
  }

  # -- Helper: unpivot rates into (standard_charge, rate_category) rows ------

  unpivot_rates <- function(raw) {
    if (nrow(raw) == 0) return(tibble())

    parts <- list()

    # Negotiated rates
    neg <- raw %>%
      filter(!is.na(standard_charge_dollar), standard_charge_dollar > 0) %>%
      mutate(standard_charge = standard_charge_dollar,
             rate_category = "negotiated")
    if (nrow(neg) > 0) parts[["negotiated"]] <- neg
    message("    Negotiated: ", format(nrow(neg), big.mark = ","))

    # Gross: deduplicate per hospital × description
    gross <- raw %>%
      filter(!is.na(gross_charge), gross_charge > 0) %>%
      distinct(hospital_id, description, ms_drg, cpt, hcpcs, .keep_all = TRUE) %>%
      mutate(standard_charge = gross_charge,
             rate_category = "gross",
             payer_name = NA_character_,
             plan_name = NA_character_)
    if (nrow(gross) > 0) parts[["gross"]] <- gross
    message("    Gross: ", format(nrow(gross), big.mark = ","))

    # Cash
    cash <- raw %>%
      filter(!is.na(discounted_cash), discounted_cash > 0) %>%
      distinct(hospital_id, description, ms_drg, cpt, hcpcs, .keep_all = TRUE) %>%
      mutate(standard_charge = discounted_cash,
             rate_category = "cash",
             payer_name = NA_character_,
             plan_name = NA_character_)
    if (nrow(cash) > 0) parts[["cash"]] <- cash
    message("    Cash: ", format(nrow(cash), big.mark = ","))

    # Min
    mn <- raw %>%
      filter(!is.na(minimum), minimum > 0) %>%
      distinct(hospital_id, description, ms_drg, cpt, hcpcs, .keep_all = TRUE) %>%
      mutate(standard_charge = minimum,
             rate_category = "min",
             payer_name = NA_character_,
             plan_name = NA_character_)
    if (nrow(mn) > 0) parts[["min"]] <- mn
    message("    Min: ", format(nrow(mn), big.mark = ","))

    # Max
    mx <- raw %>%
      filter(!is.na(maximum), maximum > 0) %>%
      distinct(hospital_id, description, ms_drg, cpt, hcpcs, .keep_all = TRUE) %>%
      mutate(standard_charge = maximum,
             rate_category = "max",
             payer_name = NA_character_,
             plan_name = NA_character_)
    if (nrow(mx) > 0) parts[["max"]] <- mx
    message("    Max: ", format(nrow(mx), big.mark = ","))

    if (length(parts) == 0) return(tibble())

    combined <- bind_rows(parts) %>%
      mutate(hcpcs_cpt = coalesce(cpt, hcpcs))

    combined %>%
      select(hospital_id, description,
             ms_drg, hcpcs_cpt,
             payer_name, plan_name,
             standard_charge, rate_category,
             setting, billing_class)
  }

  # -- Extract DRG rates -----------------------------------------------------

  if (!file.exists(drg_file)) {
    t1 <- Sys.time()
    drg_raw <- extract_by_state(drg_filter, "DRG rates")
    drg_rates <- unpivot_rates(drg_raw)
    t2 <- Sys.time()
    message("  Total DRG rows: ", format(nrow(drg_rates), big.mark = ","),
            " (", round(as.numeric(t2 - t1, units = "secs")), "s)")

    if (nrow(drg_rates) > 0) {
      write_csv(drg_rates, drg_file)
      message("  Wrote ", drg_file)
    } else {
      message("  WARNING: No DRG rate data found")
    }
  }

  # -- Extract CPT rates -----------------------------------------------------

  if (!file.exists(cpt_file)) {
    t1 <- Sys.time()
    cpt_raw <- extract_by_state(cpt_filter, "CPT/HCPCS rates")
    cpt_rates <- unpivot_rates(cpt_raw)
    t2 <- Sys.time()
    message("  Total CPT rows: ", format(nrow(cpt_rates), big.mark = ","),
            " (", round(as.numeric(t2 - t1, units = "secs")), "s)")

    if (nrow(cpt_rates) > 0) {
      write_csv(cpt_rates, cpt_file)
      message("  Wrote ", cpt_file)
    } else {
      message("  WARNING: No CPT rate data found")
    }
  }
}

# Summary -----------------------------------------------------------------

message("\nOria extraction complete.")
message("Output directory: ", out_dir)
for (f in list.files(out_dir, pattern = "\\.csv$")) {
  sz <- file.size(file.path(out_dir, f))
  message("  ", f, ": ", round(sz / 1e6, 1), " MB")
}
