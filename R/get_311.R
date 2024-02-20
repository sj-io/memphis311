#' Get Memphis 311 Data
#'
#' @param filepath The filepath to a csv file of Memphis 311 Service Requests. If none is given, will call the data from the API.
#' @param department The city department for which to call data.
#' @param date The last modified date of the requests in YYYY-MM-DD format.
#' @param cache Use cache to store data for faster future use.
#'
#' @export
#'
#' @import stringr rappdirs arrow httr2 janitor dplyr readr
#' @importFrom lubridate mdy_hms
get_311 <- function(filepath = NULL,
                    department = NULL, date = NULL,
                    cache = TRUE) {

  cache_dir <- user_cache_dir("memphis311")
  raw_folder <- file.path(cache_dir, "raw")
  parquet_file <- file.path(raw_folder, "part-0.parquet")

  # Create cache directory if it does not already exists
  if (cache == TRUE) {
    if (!file.exists(cache_dir)) {
      dir.create(cache_dir, recursive = TRUE)
      message("Created memphis311 cache.")
    }
    if (!file.exists(raw_folder)) {
      dir.create(raw_folder, recursive = TRUE)
    }
  }

  # Import the file, if using one
  if (!is.null(filepath)) {
    df <- get_311_file(filepath, department, date, cache, raw_folder)
  } else {

    # Check cache for data
    if (file.exists(parquet_file)) {

      message("Checking cache for data.")
      df <- open_dataset(raw_folder)

      if (!is.null(department)) {
        df <- df |>
          filter(department == {{ department }})
      }

      if (!is.null(date)) {
        date1 <- paste0(date, " 00:00:00")
        date2 <- paste0(date, " 23:59:59")
        df <- df |>
          filter(between(last_modified_date, ymd_hms(date1), ymd_hms(date2)))
      }

      df <- df |> collect()

      # if data isn't in cache, call API
      if (nrow(df) == 0) {
        message("Calling Memphis 311 API.")
        df <- get_311_api(department, date, cache)
      }

    } else {

      # if cache data doesn't exist, call API.
      message("Calling Memphis 311 API.")
      df <- get_311_api(department, date, cache)

    }

  }

  df

}

get_311_api <- function(department, date, cache) {

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

  # date
  if (!is.null(date)) {
    if (str_ends(url, ".json")) {
      url <- paste0(url, "?")
    } else {
      url <- paste0(url, "&")
    }

    url <- paste0(url, "$where=date_trunc_ymd(last_modified_date)%20=%27", date, "%27")
  }

  # complete url to request
  req <- httr2::request(url)

  # function which tells when there are no more responses
  # https://httr2.r-lib.org/reference/req_perform_iterative.html
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

  # convert responses to dataframe
  df <- resp |>
    resps_successes() |>
    resps_data(\(resp) resp_body_json(resp, simplifyVector = TRUE))

  df <- df |> select(any_of(ce_columns)) |> distinct()

}

get_311_file <- function(filepath, department, date, cache, raw_folder) {

  message("Reading the csv file. This might take a moment.")
  df <- read_csv(filepath) |>
    janitor::clean_names() |>
    mutate(department = str_to_lower(department),
           across(ends_with("_date"), ~ mdy_hms(.))) |>
    distinct() |>
    select(any_of(ce_columns)) |>
    arrange(desc(reported_date))

  if (cache == TRUE) {
    message("Caching the dataset for faster use in future.")
    write_dataset(df, raw_folder, max_rows_per_file = 1e7)
  }

  if (!is.null(department)) {
    df <- df |>
      filter(department == {{ department }})
  }

  if (!is.null(date)) {
    df <- df |>
      filter(reported_date == {{ date }})
  }

  df
}
