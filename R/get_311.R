#' Get Memphis 311 Data
#'
#' @param source File (the default) or API.
#'    * `"cache"`: open cached data
#'    * `"file"`: read a downloaded csv file of Memphis 311 data.
#'    * `"api"`: call the Memphis 311 API (max 1000 rows).
#' @param filepath If source = "file" and no cache data exists, the csv filepath
#' @param department The city department for which to call data
#' @param date The date of the requests
#' @param cache Use cache to store data for faster future use.
#'
#' @export
#'
#' @import stringr rappdirs lubridate arrow httr janitor dplyr readr
get_311 <- function(source = "file", filepath = NULL,
                    department = NULL, date = NULL,
                    cache = TRUE) {

  cache_dir <- user_cache_dir("memphis311")
  raw_folder <- file.path(cache_dir, "raw")

  if (cache == TRUE) {
    if (!file.exists(cache_dir)) {
      dir.create(cache_dir, recursive = TRUE)
      message("Created memphis311 cache.")
    }
    if (!file.exists(raw_folder)) {
      dir.create(raw_folder, recursive = TRUE)
    }
  }

  if (source == "file") {
    df <- get_311_file(filepath, department, date, cache, raw_folder)
  } else if (source == "api") {
    df <- get_311_api(department, date, cache)
  } else {
    df <- open_dataset(raw_folder)
  }

  df

}

get_311_api <- function(department, date, cache) {

  url <- "https://data.memphistn.gov/resource/hmd4-ddta.json"

  # TODO: add null option to department
  if (!is.null(department)) {

    dept <- str_to_title(department)
    if (department == "parks and neighborhoods") {
      dept <- "Parks and Neighborhoods"
    }
    dept <- gsub(" ", "%20", dept)

    url <- paste0(url, "?department=", dept)

  }

  df <- httr::GET(url, progress())

}

get_311_file <- function(filepath, department, date, cache, raw_folder) {

  df <- read_csv(filepath) |>
    janitor::clean_names() |>
    mutate(department = str_to_lower(department),
           across(ends_with("_date"), ~ mdy_hms(.))) |>
    distinct() |>
    arrange(desc(reported_date))

  if (cache == TRUE) {
    message("Caching the dataset for faster use in future.")
    write_dataset(df, raw_folder)
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
