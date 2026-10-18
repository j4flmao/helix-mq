#!/usr/bin/env bash
# Environment-aware docker compose wrapper. Each environment selects its compose
# file pair and its .env file:
#
#   scripts/compose.sh dev up -d --build      standalone broker + example walkthrough
#   scripts/compose.sh staging up -d          production-like durability, no demo
#   scripts/compose.sh prod up -d --build     read-only rootfs, restart=always
#   scripts/compose.sh cluster up -d --build  three-node replicated cluster
#
#   up | down | logs | ps | build | restart   passed straight through
#   demo                                      re-run the walkthrough container
#   test                                      unit + config tests in a container
#   shell                                     toolchain shell, repo mounted at /src
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

env=${1:-}
[[ $# -gt 0 ]] && shift
action=${1:-ps}
[[ $# -gt 0 ]] && shift

case "$env" in
    dev | staging | prod)
        files=(-f docker-compose.yml -f "docker-compose.$env.yml")
        ;;
    cluster)
        files=(-f docker-compose.cluster.yml)
        ;;
    "")
        echo "usage: scripts/compose.sh <dev|staging|prod|cluster> <action> [args...]" >&2
        exit 2
        ;;
    *)
        echo "unknown environment '$env' (dev, staging, prod, cluster)" >&2
        exit 2
        ;;
esac

envfile=".env.$env"
# Export the environment file so COMPOSE_PROJECT_NAME and friends apply.
set -a
# shellcheck disable=SC1090
source "$envfile"
set +a

compose() {
    docker compose --env-file "$envfile" "${files[@]}" "$@"
}

case "$action" in
    up | down | logs | ps | build | restart)
        compose "$action" "$@"
        ;;
    demo)
        case "$env" in
            dev | cluster)
                compose run --rm demo
                ;;
            *)
                echo "the '$env' environment has no demo service; use dev or cluster" >&2
                exit 2
                ;;
        esac
        ;;
    test)
        compose --profile tests run --rm tests
        ;;
    shell)
        compose --profile tests run --rm shell
        ;;
    *)
        echo "unknown action '$action' (up, down, logs, ps, build, restart, demo, test, shell)" >&2
        exit 2
        ;;
esac
