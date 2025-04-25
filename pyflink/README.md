# PyFlink Docker Environment

This directory contains the `docker-compose.yml` file used to set up the Flink and Redpanda services for the workshop.

PyFlink job scripts should generally be placed in the project root directory or a subdirectory like `../jobs/` so they are accessible via the `/app` volume mount defined in `docker-compose.yml`.

Refer to the main `../README.md` for setup and usage instructions. 