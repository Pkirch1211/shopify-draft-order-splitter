# Shopify Draft Order Splitter

This repo runs a scheduled script that reviews Shopify draft orders and splits backordered items into child drafts.

## What it does
- Reviews open draft orders
- Skips drafts already processed
- Skips child drafts
- Splits eligible backordered items into child drafts
- Prints a run summary with success, skipped, and failed drafts

## Required GitHub Secrets
- SHOPIFY_TOKEN
- SHOPIFY_STORE
- LOCATION_ID

## Required GitHub Variables
- API_VERSION
- DRY_RUN
- LOOKBACK_DAYS
- IDEMPOTENCY_DONE_TAG
- CHILD_TAG

## Run manually
Use the GitHub Actions workflow_dispatch trigger.

## Schedule
The workflow can also run on a cron schedule through GitHub Actions.
