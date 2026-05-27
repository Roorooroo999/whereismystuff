# Deploy Where's My Stuff Dashboard to Posit Connect
# Run this script in R/RStudio to publish the dashboard

# Install required packages if not already installed
if (!require("rsconnect")) install.packages("rsconnect")
if (!require("quarto")) install.packages("quarto")

library(rsconnect)

# ============================================
# STEP 1: Configure Posit Connect Account (One-time setup)
# ============================================
# Get your API key from: https://posit.wal-mart.com/__api__/me
# Then run:
#
# rsconnect::setAccountInfo(
#   name = "your_username",
#   token = "YOUR_API_KEY",
#   secret = "YOUR_SECRET",
#   server = "posit.wal-mart.com"
# )

# ============================================
# STEP 2: Deploy the Dashboard
# ============================================

# Option A: Deploy Quarto Dashboard (Shiny-based)
# This deploys the interactive R/Shiny dashboard
deploy_quarto_dashboard <- function() {
  cat("Deploying Quarto Dashboard to Posit Connect...\n")

  # Set working directory to dashboard folder
  setwd(dirname(rstudioapi::getSourceEditorContext()$path))

  # Deploy using rsconnect
  rsconnect::deployDoc(
    doc = "wheres_my_stuff_dashboard.qmd",
    appTitle = "Where's My Stuff - Inventory Dashboard (Posit)",
    account = NULL,  # Uses default account
    server = "posit.wal-mart.com",
    forceUpdate = TRUE
  )

  cat("Deployment complete!\n")
}

# Option B: Deploy via Quarto CLI (if Quarto is installed)
deploy_via_quarto_cli <- function() {
  cat("Deploying via Quarto CLI...\n")
  system("quarto publish connect wheres_my_stuff_dashboard.qmd --no-browser")
}

# ============================================
# STEP 3: Set Environment Variables in Posit Connect
# ============================================
# After deployment, go to your content settings and add:
# - GOOGLE_APPLICATION_CREDENTIALS = /path/to/service-account.json
# OR use Workload Identity Federation

# ============================================
# RUN DEPLOYMENT
# ============================================
# Uncomment one of these lines to deploy:

# deploy_quarto_dashboard()
# deploy_via_quarto_cli()

cat("\n")
cat("=========================================\n")
cat("  Where's My Stuff - Posit Deployment\n")
cat("=========================================\n")
cat("\n")
cat("To deploy, run ONE of these in R console:\n")
cat("\n")
cat("  1. deploy_quarto_dashboard()   # Uses rsconnect\n")
cat("  2. deploy_via_quarto_cli()     # Uses Quarto CLI\n")
cat("\n")
cat("First time? Set up your account:\n")
cat("  rsconnect::setAccountInfo(\n")
cat("    name = 'your_username',\n")
cat("    token = 'YOUR_API_KEY',\n")
cat("    secret = 'YOUR_SECRET',\n")
cat("    server = 'posit.wal-mart.com'\n")
cat("  )\n")
cat("\n")
cat("Get API key at: https://posit.wal-mart.com/__api__/me\n")
cat("=========================================\n")
