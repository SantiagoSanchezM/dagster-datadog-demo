# Template for use with the 1Password CLI (`op run`) instead of a plaintext .env file.
# Replace the op://<vault>/<item>/<field> placeholders below with references to your own
# 1Password vault item, then run:
#
#   op run --env-file=.env.1password.tpl -- docker compose up -d --build
#
# `op run` resolves these references at process start and injects the real values only into
# that docker compose process's environment - nothing is ever written to disk as plaintext.
# See: https://developer.1password.com/docs/cli/secrets-environment-variables/

DD_API_KEY=op://<vault>/<item>/<field>
OPENLINEAGE_API_KEY=op://<vault>/<item>/<field>
DD_SITE=datadoghq.com
OPENLINEAGE_URL=https://data-obs-intake.datadoghq.com
OL_ENV_TAG=prod

POSTGRES_USER=dagster
POSTGRES_PASSWORD=dagster
POSTGRES_DB=dagster
