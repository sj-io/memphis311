#' Get Memphis 311 Data
#'
#' @param department The city department for which to call data.
#' @param creation_date The date the sr was created in YYYY-MM-DD format.
#' @param last_modified_date The last modified date of the requests in YYYY-MM-DD format.
#' @param save_cache Save data in cache for faster future use.
#' @param use_cache Use existing cache data. Set to FALSE to call API data.
#'
#' @export
#'
#' @import stringr rappdirs arrow httr2 janitor dplyr
#' @importFrom lubridate mdy_hms ymd_hms
#' @importFrom httr GET write_disk progress
get_311 <- function(department = NULL, creation_date = NULL, last_modified_date = NULL,
                    save_cache = TRUE, use_cache = TRUE) {

  cache_dir <- user_cache_dir("memphis311")
  raw_folder <- file.path(cache_dir, "raw")
  parquet_file <- file.path(raw_folder, "part-0.parquet")

  # Create cache directory if it does not already exists
  if (save_cache == TRUE) {
    if (!file.exists(cache_dir)) {
      dir.create(cache_dir, recursive = TRUE)
      message("Created memphis311 cache.")
    }
    if (!file.exists(raw_folder)) {
      dir.create(raw_folder, recursive = TRUE)
    }
  }

  if(!all(is.null(c(department, creation_date, last_modified_date)))) {
    if (use_cache == TRUE & file.exists(parquet_file)) {

      message("Checking cache for data.")
      parquet_dataset <- open_dataset(raw_folder, schema = the_schema)
      df <- parquet_dataset

      df <- filter_variables(df, department, creation_date, last_modified_date)

      df <- df |> collect()

      # if data isn't in cache, call API
      if (nrow(df) == 0) {
        message("Calling Memphis 311 API.")
        df <- get_311_api(department, creation_date, last_modified_date)

        # update cache
        if (save_cache == TRUE) {
          df <- add_missing_columns(df)
          updated_cache <- concat_tables(as_arrow_table(parquet_dataset, schema = the_schema), as_arrow_table(df, schema = the_schema))
          write_dataset(updated_cache, raw_folder, max_rows_per_file = 1e7)
        }
      }

    } else {

      # if cache data doesn't exist, call API.
      message("Calling Memphis 311 API.")
      df <- get_311_api(department, creation_date, last_modified_date)

      if (save_cache == TRUE) {
        # add missing columns to data
        df <- add_missing_columns(df)
        save_df <- as_arrow_table(df, schema = the_schema)
        write_dataset(save_df, raw_folder, max_rows_per_file = 1e7)
      }

    }

  } else {
    # if downloading the entire dataset, use a separate URL
    temp_csv <- file.path(user_cache_dir("memphis311"), "temp.csv")

    message("Getting entire dataset. This will take a while.")
    httr::GET("https://data.memphistn.gov/api/views/hmd4-ddta/rows.csv?accessType=DOWNLOAD&api_foundry=true",
              httr::write_disk(temp_csv), httr::progress())

    df <- read_csv_arrow(temp_csv, as_data_frame = FALSE) |>
      janitor::clean_names() |>
      select(any_of(ce_columns)) |>
      distinct() |>
      arrange(desc(reported_date)) |>
      collect()

    df <- df |>
      mutate(department = str_to_lower(department),
             across(ends_with("_date"), ~ mdy_hms(.)),
             across(c(followup_date, mlgw_on), ~ as.Date(.)))

    if (save_cache == TRUE) {
      message("Caching the dataset for faster use in future.")
      save_df <- as_arrow_table(df, schema = the_schema)
      write_dataset(save_df, raw_folder, max_rows_per_file = 1e7)
      rm(save_df)
    }

    df <- filter_variables(df, department, creation_date, last_modified_date)

    unlink(temp_csv)
  }

  df

}

get_311_api <- function(department, creation_date, last_modified_date) {

  url <- "https://data.memphistn.gov/resource/hmd4-ddta.json"

  # narrow call based on user parameters
  # department
  if (!is.null(department)) {

    dept <- str_to_title(department)
    if (department == "parks and neighborhoods") {
      dept <- "Parks and Neighborhoods"
    }
    dept <- gsub(" ", "%20", dept)

    url <- paste0(url, "?department=", dept)

  }

  # last_modified_date
  if (!is.null(last_modified_date)) {
    if (str_ends(url, ".json")) {
      url <- paste0(url, "?")
    } else {
      url <- paste0(url, "&")
    }

    url <- paste0(url, "$where=date_trunc_ymd(last_modified_date)%20=%27", last_modified_date, "%27")
  }

  # creation_date
  if (!is.null(creation_date)) {
    if (str_ends(url, ".json")) {
      url <- paste0(url, "?")
    } else {
      url <- paste0(url, "&")
    }

    url <- paste0(url, "$where=date_trunc_ymd(creation_date)%20=%27", creation_date, "%27")
  }

  # request data
  req <- httr2::request(url)

  # function which tells when there are no more responses
  # https://httr2.r-lib.org/reference/iterate_with_offset.html
  is_complete <- function(resp) {
    length(resp_body_json(resp)$data) == 0
  }

  # socrata limits requests to 1000. perform the call until no valid responses
  # https://support.socrata.com/hc/en-us/articles/202949268-How-to-query-more-than-1000-rows-of-a-dataset
  resp <- req_perform_iterative(
    req,
    next_req = iterate_with_offset("offset", offset = 1000, resp_complete = is_complete),
    max_reqs = Inf
    )

  # convert responses to table
  df <- resp |>
    resps_successes() |>
    resps_data(\(resp) resp_body_json(resp, simplifyVector = TRUE))

  if (length(df) == 0) {
    stop("Your API call returned no results.")
  }

  df <- df |> select(any_of(ce_columns)) |> distinct()

}

add_missing_columns <- function(df) {
  missing_cols <- setdiff(ce_columns, names(df))

  df[missing_cols] <- NA_character_

  df <- df |>
    mutate(
      across(c(incident_type_id,
               incident_number,
               incident_id,
               created_by,
               number_of_tasks,
               last_update_login), ~ as.numeric(.)),
      across(c(creation_date,
               reported_date,
               last_modified_date,
               last_update_date,
               close_date,
               incident_resolved_date,
               next_open_task_date), ~ ymd_hms(.)
      ),
      across(c(followup_date, mlgw_on), ~ as.Date(.)),
      department = str_to_lower(department)
    )

  df[ce_columns]
}

filter_variables <- function(df, department, creation_date, last_modified_date) {
  if (!is.null(department)) {
    df <- df |>
      filter(department == {{ department }})
  }

  if (!is.null(creation_date)) {
    creation_date1 <- paste0(creation_date, " 00:00:00")
    creation_date2 <- paste0(creation_date, " 23:59:59")
    df <- df |>
      filter(between(creation_date, ymd_hms(creation_date1), ymd_hms(creation_date2)))
  }

  if (!is.null(last_modified_date)) {
    last_modified_date1 <- paste0(last_modified_date, " 00:00:00")
    last_modified_date2 <- paste0(last_modified_date, " 23:59:59")
    df <- df |>
      filter(between(last_modified_date, ymd_hms(last_modified_date1), ymd_hms(last_modified_date2)))
  }

  df
}
