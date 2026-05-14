# Install required R packages for the dashboard
# Run this script once before deploying

packages <- c(
  "shiny",
  "bigrquery",
  "DBI",
  "dplyr",
  "ggplot2",
  "plotly",
  "DT",
  "scales",
  "tidyr",
  "quarto"
)

install_if_missing <- function(pkg) {
  if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
    install.packages(pkg, repos = "https://cran.rstudio.com/")
  }
}

invisible(lapply(packages, install_if_missing))

cat("All packages installed successfully!\n")
