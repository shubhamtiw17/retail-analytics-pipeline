# Security Policy

## Reporting a vulnerability

If you discover a security vulnerability in this project, please do not
open a public GitHub issue. Instead, contact the maintainer directly.

## Credentials in this repo

This repository uses example credentials for local development only:

- MinIO: `admin / password123` — local Docker only, never production
- PostgreSQL: `retail / retail123` — local Docker only, never production
- Airflow: `admin / admin` — local Docker only, never production

All real credentials are stored in `.env` which is gitignored and never
committed. The `.env.example` file contains only placeholder values.

## What this project does NOT contain

- No real customer data
- No production API keys
- No cloud provider credentials
- No personally identifiable information