
<!-- README.md is generated from README.Rmd. Please edit that file -->

# memphis311

<!-- badges: start -->
<!-- badges: end -->

The goal of memphis311 is to access (and eventually, tidy) Memphis 311
Service Requests from the [public
dataset](https://data.memphistn.gov/dataset/Service-Requests-since-2016/hmd4-ddta/about_data).

## Installation

You can install the development version of memphis311 like so:

``` r
# install.packages("devtools")
devtools::install_github("sj-io/memphis311")
```

## Get 311 data

To use the package, first load it into your library.

``` r
library(memphis311)
```

To download the entire dataset, use `get_311()`. I currently recommend
this if you expect over 1000 rows of data.

``` r
all_311_requests <- get_311()
```

Note the entire dataset is over 2 million rows, so this will take a
minute. The data is saved to your cache folder, so you can access it in
the future.

You can currently query using the following parameters:

``` r
ce_240129 <- get_311(department = "code enforcement", creation_date = "2024-01-29", last_modified_date = NULL)
```

If the requested data isn’t found in the cache, it’ll call the API.
Warning: Some API requests do not return all of the data. Working on
this. Downloading the entire dataset first will catch everything.
