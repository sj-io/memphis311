# cache_dir <- user_cache_dir("memphis311")
# raw_folder <- file.path(cache_dir, "raw")
# parquet_file <- file.path(raw_folder, "part-0.parquet")
#
# # Create cache directory if it does not already exists
# if (save_cache == TRUE) {
#   if (!file.exists(cache_dir)) {
#     dir.create(cache_dir, recursive = TRUE)
#     message("Created memphis311 cache.")
#   }
#   if (!file.exists(raw_folder)) {
#     dir.create(raw_folder, recursive = TRUE)
#   }
# }

# if (use_cache == TRUE & file.exists(parquet_file)) {
#
#   message("Checking cache for data.")
#   parquet_dataset <- open_dataset(raw_folder, schema = the_schema)
#   df <- parquet_dataset
#
#   df <- filter_variables(df, department, creation_date, last_modified_date)
#
#   df <- df |> collect()

# if data isn't in cache, call API
# if (nrow(df) == 0) {
#   message("Calling Memphis 311 API.")
#
# }
# update cache
# if (save_cache == TRUE) {
#   df <- add_missing_columns(df)
#   updated_cache <- concat_tables(as_arrow_table(parquet_dataset, schema = the_schema), as_arrow_table(df, schema = the_schema))
#   write_dataset(updated_cache, raw_folder, max_rows_per_file = 1e7)
# }

# if (save_cache == TRUE) {
#   message("Caching the dataset for faster use in future.")
#   save_df <- as_arrow_table(df, schema = the_schema)
#   write_dataset(save_df, raw_folder, max_rows_per_file = 1e7)
#   rm(save_df)
# }

# add_missing_columns <- function(df) {
#   missing_cols <- setdiff(ce_columns, names(df))
#
#   df[missing_cols] <- NA_character_
#
#   df <- df |>
#     mutate(
#       across(c(incident_type_id,
#                incident_number,
#                incident_id,
#                created_by,
#                number_of_tasks,
#                last_update_login), ~ as.numeric(.)),
#       across(c(creation_date,
#                reported_date,
#                last_modified_date,
#                last_update_date,
#                close_date,
#                incident_resolved_date,
#                next_open_task_date), ~ ymd_hms(.)
#       ),
#       across(c(followup_date, mlgw_on), ~ as.Date(.)),
#       department = str_to_lower(department)
#     )
#
#   df[ce_columns]
# }

# df <- filter_variables(df, department, creation_date, last_modified_date)
#
# filter_variables <- function(df, department, creation_date, last_modified_date) {
#   if (!is.null(department)) {
#     df <- df |>
#       filter(department == {{ department }})
#   }
#
#   if (!is.null(creation_date)) {
#     creation_date1 <- paste0(creation_date, " 00:00:00")
#     creation_date2 <- paste0(creation_date, " 23:59:59")
#     df <- df |>
#       filter(between(creation_date, ymd_hms(creation_date1), ymd_hms(creation_date2)))
#   }
#
#   if (!is.null(last_modified_date)) {
#     last_modified_date1 <- paste0(last_modified_date, " 00:00:00")
#     last_modified_date2 <- paste0(last_modified_date, " 23:59:59")
#     df <- df |>
#       filter(between(last_modified_date, ymd_hms(last_modified_date1), ymd_hms(last_modified_date2)))
#   }
#
#   df
# }

# the_schema <- schema(
#   division = string(),
#   department = string(),
#   group_name = string(),
#   category = string(),
#   ce_category = string(),
#   request_type = string(),
#   incident_type_id = double(),
#   incident_number = double(),
#   incident_id = double(),
#   resolution_code = string(),
#   resolution_code_meaning = string(),
#   resolution_summary = string(),
#   summary_1 = string(),
#   request_status = string(),
#   request_priority = string(),
#   creation_date = timestamp(unit = "us", timezone = "UTC"),
#   reported_date = timestamp(unit = "us", timezone = "UTC"),
#   last_modified_date = timestamp(unit = "us", timezone = "UTC"),
#   last_update_date = timestamp(unit = "us", timezone = "UTC"),
#   followup_date = date32(),
#   close_date = timestamp(unit = "us", timezone = "UTC"),
#   incident_resolved_date = timestamp(unit = "us", timezone = "UTC"),
#   next_open_task_date = timestamp(unit = "us", timezone = "UTC"),
#   sr_creation_channel = string(),
#   created_by = double(),
#   created_by_user = string(),
#   owner_name = string(),
#   number_of_tasks = double(),
#   last_update_login = double(),
#   last_updated_by = string(),
#   mlgw_status = string(),
#   mlgw_on = date32(),
#   parcel_id = string(),
#   street_name = string(),
#   full_address = string(),
#   location_1 = string(),
#   address1 = string(),
#   address2 = string(),
#   address3 = string(),
#   city = string(),
#   state = string(),
#   postal_code = string(),
#   district = string(),
#   sub_district = string(),
#   map_page = string(),
#   target_block = string(),
#   zone = string(),
#   area = string(),
#   collection_day = string(),
#   swm_code = string()
# )
