#!/usr/bin/env bash

# User-provided configuration must always be respected.
# Therefore, this script must only derives Airflow AIRFLOW__ variables from other variables
# when the user did not provide their own configuration.

TRY_LOOP="20"

# Global defaults and back-compat
: "${AIRFLOW_HOME:="/usr/local/airflow"}"
: "${AIRFLOW__CORE__FERNET_KEY:=${FERNET_KEY:=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")}}"
: "${AIRFLOW__CORE__EXECUTOR:=${EXECUTOR:-Sequential}Executor}"

# Tambahkan variabel MONGO_URI jika tidak didefinisikan oleh pengguna
: "${MONGO_URI:="mongodb://admin:admin@mongo:27017/airflow"}"

# Load DAGs examples (default: Yes)
if [[ -z "$AIRFLOW__CORE__LOAD_EXAMPLES" && "${LOAD_EX:=n}" == n ]]; then
  AIRFLOW__CORE__LOAD_EXAMPLES=False
fi

export \
  AIRFLOW_HOME \
  AIRFLOW__CORE__EXECUTOR \
  AIRFLOW__CORE__FERNET_KEY \
  AIRFLOW__CORE__LOAD_EXAMPLES \
  MONGO_URI  # Export MONGO_URI agar bisa digunakan di aplikasi

# Install custom python package if requirements.txt is present
if [ -e "/requirements.txt" ]; then
    $(command -v pip) install --user -r /requirements.txt
fi

wait_for_port() {
  local name="$1" host="$2" port="$3"
  local j=0
  while ! nc -z "$host" "$port" >/dev/null 2>&1 < /dev/null; do
    j=$((j+1))
    if [ $j -ge $TRY_LOOP ]; then
      echo >&2 "$(date) - $host:$port still not reachable, giving up"
      exit 1
    fi
    echo "$(date) - waiting for $name... $j/$TRY_LOOP"
    sleep 5
  done
}

# Check if the user has provided explicit Airflow configuration concerning the MongoDB
if [ -n "$MONGO_URI" ]; then
  # Gunakan `MONGO_URI` di sini jika aplikasi atau script Anda memerlukan koneksi ke MongoDB.
  echo "Using MongoDB URI: $MONGO_URI"
fi

# PostgreSQL untuk backend Airflow
if [ "$AIRFLOW__CORE__EXECUTOR" != "SequentialExecutor" ]; then
  if [ -z "$AIRFLOW__CORE__SQL_ALCHEMY_CONN" ]; then
    : "${POSTGRES_HOST:="postgres"}"
    : "${POSTGRES_PORT:="5432"}"
    : "${POSTGRES_USER:="airflow"}"
    : "${POSTGRES_PASSWORD:="airflow"}"
    : "${POSTGRES_DB:="airflow"}"
    : "${POSTGRES_EXTRAS:-""}"

    AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}${POSTGRES_EXTRAS}"
    export AIRFLOW__CORE__SQL_ALCHEMY_CONN

    if [ "$AIRFLOW__CORE__EXECUTOR" = "CeleryExecutor" ]; then
      AIRFLOW__CELERY__RESULT_BACKEND="db+postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}${POSTGRES_EXTRAS}"
      export AIRFLOW__CELERY__RESULT_BACKEND
    fi
  fi
  wait_for_port "Postgres" "$POSTGRES_HOST" "$POSTGRES_PORT"
fi

# CeleryExecutor membutuhkan Redis sebagai broker
if [ "$AIRFLOW__CORE__EXECUTOR" = "CeleryExecutor" ]; then
  if [ -z "$AIRFLOW__CELERY__BROKER_URL" ]; then
    : "${REDIS_PROTO:="redis://"}"
    : "${REDIS_HOST:="redis"}"
    : "${REDIS_PORT:="6379"}"
    : "${REDIS_PASSWORD:=""}"
    : "${REDIS_DBNUM:="1"}"

    if [ -n "$REDIS_PASSWORD" ]; then
      REDIS_PREFIX=":${REDIS_PASSWORD}@"
    else
      REDIS_PREFIX=
    fi

    AIRFLOW__CELERY__BROKER_URL="${REDIS_PROTO}${REDIS_PREFIX}${REDIS_HOST}:${REDIS_PORT}/${REDIS_DBNUM}"
    export AIRFLOW__CELERY__BROKER_URL
  fi
  wait_for_port "Redis" "$REDIS_HOST" "$REDIS_PORT"
fi

# Case logic to start services
case "$1" in
  webserver)
    airflow initdb
    if [ "$AIRFLOW__CORE__EXECUTOR" = "LocalExecutor" ] || [ "$AIRFLOW__CORE__EXECUTOR" = "SequentialExecutor" ]; then
      airflow scheduler &
    fi
    exec airflow webserver
    ;;
  worker|scheduler)
    sleep 10
    exec airflow "$@"
    ;;
  flower)
    sleep 10
    exec airflow "$@"
    ;;
  version)
    exec airflow "$@"
    ;;
  *)
    exec "$@"
    ;;
esac
