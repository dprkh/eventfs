#!/usr/bin/env bash
set -euo pipefail

readonly DEFAULT_IMAGE="eventfs-linux:dev"

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

RUN_IMAGE=""
RUN_CPUS=""
RUN_MEMORY=""
RUN_BUILD="0"
RUN_ARGS=()

BUILD_IMAGE_NAME=""
BUILD_CPUS=""
BUILD_MEMORY=""

usage() {
    cat <<'USAGE'
Usage:
  ./dev.sh test [--build] [--image IMAGE] [--cpus N] [--memory SIZE]
  ./dev.sh check [--build] [--image IMAGE] [--cpus N] [--memory SIZE]
  ./dev.sh lib [--build] [--image IMAGE] [--cpus N] [--memory SIZE]
  ./dev.sh tests [--build] [--image IMAGE] [--cpus N] [--memory SIZE]
  ./dev.sh examples [--build] [--image IMAGE] [--cpus N] [--memory SIZE]
  ./dev.sh benches [--build] [--image IMAGE] [--cpus N] [--memory SIZE]
  ./dev.sh clippy [--build] [--image IMAGE] [--cpus N] [--memory SIZE]
  ./dev.sh cargo [--build] [--image IMAGE] [--cpus N] [--memory SIZE] -- <args...>
  ./dev.sh shell [--build] [--image IMAGE] [--cpus N] [--memory SIZE] -- <args...>
  ./dev.sh fmt
  ./dev.sh build-image [--image IMAGE] [--cpus N] [--memory SIZE]
  ./dev.sh help
USAGE
}

die() {
    echo "error: $*" >&2
    exit 1
}

default_cpus() {
    local cpus=""

    if command -v sysctl >/dev/null 2>&1; then
        cpus="$(sysctl -n hw.ncpu 2>/dev/null || true)"
    fi
    if [[ -z "${cpus//[[:space:]]/}" ]] && command -v nproc >/dev/null 2>&1; then
        cpus="$(nproc 2>/dev/null || true)"
    fi

    cpus="${cpus//[[:space:]]/}"
    if [[ -z "${cpus}" ]]; then
        printf '1'
    else
        printf '%s' "${cpus}"
    fi
}

default_memory() {
    local bytes=""
    local gib

    if command -v sysctl >/dev/null 2>&1; then
        bytes="$(sysctl -n hw.memsize 2>/dev/null || true)"
    fi

    bytes="${bytes//[[:space:]]/}"
    if [[ -z "${bytes}" || ! "${bytes}" =~ ^[0-9]+$ ]]; then
        printf '8G'
        return
    fi

    gib=$((bytes / 1073741824))
    if ((gib < 1)); then
        gib=1
    fi
    printf '%sG' "${gib}"
}

require_option_value() {
    local option="$1"
    local value="${2:-}"

    if [[ -z "${value}" ]]; then
        die "${option} requires a value"
    fi
}

parse_run_options() {
    RUN_IMAGE="${DEFAULT_IMAGE}"
    RUN_CPUS="$(default_cpus)"
    RUN_MEMORY="$(default_memory)"
    RUN_BUILD="0"
    RUN_ARGS=()

    while (($# > 0)); do
        case "$1" in
            --build)
                RUN_BUILD="1"
                shift
                ;;
            --image)
                require_option_value "$1" "${2:-}"
                RUN_IMAGE="$2"
                shift 2
                ;;
            --image=*)
                RUN_IMAGE="${1#*=}"
                require_option_value "--image" "${RUN_IMAGE}"
                shift
                ;;
            --cpus)
                require_option_value "$1" "${2:-}"
                RUN_CPUS="$2"
                shift 2
                ;;
            --cpus=*)
                RUN_CPUS="${1#*=}"
                require_option_value "--cpus" "${RUN_CPUS}"
                shift
                ;;
            --memory)
                require_option_value "$1" "${2:-}"
                RUN_MEMORY="$2"
                shift 2
                ;;
            --memory=*)
                RUN_MEMORY="${1#*=}"
                require_option_value "--memory" "${RUN_MEMORY}"
                shift
                ;;
            -h | --help)
                usage
                exit 0
                ;;
            --)
                shift
                RUN_ARGS=("$@")
                return
                ;;
            *)
                RUN_ARGS+=("$1")
                shift
                ;;
        esac
    done
}

parse_build_options() {
    BUILD_IMAGE_NAME="${DEFAULT_IMAGE}"
    BUILD_CPUS="$(default_cpus)"
    BUILD_MEMORY="$(default_memory)"

    while (($# > 0)); do
        case "$1" in
            --image)
                require_option_value "$1" "${2:-}"
                BUILD_IMAGE_NAME="$2"
                shift 2
                ;;
            --image=*)
                BUILD_IMAGE_NAME="${1#*=}"
                require_option_value "--image" "${BUILD_IMAGE_NAME}"
                shift
                ;;
            --cpus)
                require_option_value "$1" "${2:-}"
                BUILD_CPUS="$2"
                shift 2
                ;;
            --cpus=*)
                BUILD_CPUS="${1#*=}"
                require_option_value "--cpus" "${BUILD_CPUS}"
                shift
                ;;
            --memory)
                require_option_value "$1" "${2:-}"
                BUILD_MEMORY="$2"
                shift 2
                ;;
            --memory=*)
                BUILD_MEMORY="${1#*=}"
                require_option_value "--memory" "${BUILD_MEMORY}"
                shift
                ;;
            -h | --help)
                usage
                exit 0
                ;;
            --build)
                die "--build is not valid for build-image"
                ;;
            *)
                die "unexpected argument for build-image: $1"
                ;;
        esac
    done
}

validate_resources() {
    local cpus="$1"
    local memory="$2"

    if [[ ! "${cpus}" =~ ^[0-9]+$ || "${cpus}" -le 0 ]]; then
        die "invalid CPU count: ${cpus}"
    fi
    if [[ -z "${memory}" ]]; then
        die "memory must not be empty"
    fi
}

host_ready() {
    local cpus="$1"
    local memory="$2"
    local status

    validate_resources "${cpus}" "${memory}"

    if [[ "$(uname -s)" != "Darwin" ]]; then
        die "this runner is for macOS hosts using Apple container"
    fi
    if ! command -v container >/dev/null 2>&1; then
        die "Apple container CLI is not on PATH"
    fi

    status="$(container system status --format json 2>/dev/null || true)"
    if [[ "${status}" != *'"status":"running"'* ]]; then
        echo "starting Apple container service" >&2
        container system start --enable-kernel-install
        status="$(container system status --format json 2>/dev/null || true)"
        if [[ "${status}" != *'"status":"running"'* ]]; then
            die "Apple container service is not running"
        fi
    fi
}

host_fmt_ready() {
    if ! command -v cargo >/dev/null 2>&1; then
        die "cargo is not on PATH"
    fi
    if ! cargo fmt --version >/dev/null 2>&1; then
        die "cargo fmt is unavailable; install the rustfmt component"
    fi
}

build_image() {
    local image="$1"
    local cpus="$2"
    local memory="$3"

    host_ready "${cpus}" "${memory}"

    echo "building ${image} with ${cpus} CPUs and ${memory} memory" >&2
    cd "${REPO_ROOT}"
    container build \
        --cpus "${cpus}" \
        --memory "${memory}" \
        --build-arg "CARGO_BUILD_JOBS=${cpus}" \
        --file Containerfile \
        --tag "${image}" \
        .
}

ensure_image() {
    local image="$1"
    local cpus="$2"
    local memory="$3"
    local build="$4"

    case "${build}" in
        1)
            build_image "${image}" "${cpus}" "${memory}"
            ;;
        0)
            if ! container image inspect "${image}" >/dev/null 2>&1; then
                echo "error: image ${image} is not available; run: ./dev.sh build-image --image ${image}; or rerun with --build" >&2
                exit 1
            fi
            echo "reusing existing image ${image}" >&2
            ;;
        *)
            die "--build must be omitted or passed as a flag"
            ;;
    esac
}

run_in_container() {
    local image="$1"
    local cpus="$2"
    local memory="$3"
    local inside_command="$4"
    shift 4

    container run \
        --rm \
        --cpus "${cpus}" \
        --memory "${memory}" \
        --cap-add SYS_ADMIN \
        --mount "type=bind,source=${REPO_ROOT},target=/work" \
        --env "CARGO_HOME=/work/target/container/cargo-home" \
        --env "CARGO_TARGET_DIR=/work/target/container/target" \
        --env "CARGO_BUILD_JOBS=${cpus}" \
        --env "RUST_TEST_THREADS=1" \
        "${image}" \
        bash -lc '
            set -euo pipefail

            if [[ "$(id -u)" -eq 0 ]]; then
                if [[ -e /dev/fuse ]]; then
                    chmod 0666 /dev/fuse || true
                fi

                if [[ -f /etc/fuse.conf ]]; then
                    if grep -q "^#user_allow_other" /etc/fuse.conf; then
                        sed -i "s/^#user_allow_other/user_allow_other/" /etc/fuse.conf
                    elif ! grep -q "^user_allow_other" /etc/fuse.conf; then
                        printf "\nuser_allow_other\n" >> /etc/fuse.conf
                    fi
                fi
            fi

            export PATH="/usr/local/cargo/bin:${PATH}"
            export CARGO_HOME="${CARGO_HOME:-/work/target/container/cargo-home}"
            export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/work/target/container/target}"
            export RUST_TEST_THREADS="${RUST_TEST_THREADS:-1}"
            export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-$(nproc)}"

            cd /work
            exec bash /work/dev.sh "$@"
        ' bash "${inside_command}" "$@"
}

run_container_task() {
    local task="$1"
    local allow_args="$2"
    shift 2

    parse_run_options "$@"

    if [[ "${allow_args}" == "0" && "${#RUN_ARGS[@]}" -ne 0 ]]; then
        die "unexpected argument for ${task}: ${RUN_ARGS[0]}"
    fi

    host_ready "${RUN_CPUS}" "${RUN_MEMORY}"
    ensure_image "${RUN_IMAGE}" "${RUN_CPUS}" "${RUN_MEMORY}" "${RUN_BUILD}"
    run_in_container "${RUN_IMAGE}" "${RUN_CPUS}" "${RUN_MEMORY}" "__inside-fuse-check"
    if [[ "${#RUN_ARGS[@]}" -eq 0 ]]; then
        run_in_container "${RUN_IMAGE}" "${RUN_CPUS}" "${RUN_MEMORY}" "__inside-${task}"
    else
        run_in_container "${RUN_IMAGE}" "${RUN_CPUS}" "${RUN_MEMORY}" "__inside-${task}" "${RUN_ARGS[@]}"
    fi
}

run_fmt() {
    if (($# > 0)); then
        case "$1" in
            -h | --help)
                usage
                return
                ;;
            *)
                die "unexpected argument for fmt: $1"
                ;;
        esac
    fi

    host_fmt_ready
    cd "${REPO_ROOT}"
    cargo fmt --all -- --check
}

run_build_image_command() {
    parse_build_options "$@"
    build_image "${BUILD_IMAGE_NAME}" "${BUILD_CPUS}" "${BUILD_MEMORY}"
}

require_linux_inside() {
    if [[ "$(uname -s)" != "Linux" ]]; then
        die "internal command must run inside the Linux container"
    fi
}

inside_fuse_check() {
    if [[ ! -c /dev/fuse ]]; then
        echo "missing /dev/fuse" >&2
        exit 1
    fi

    if [[ ! -r /dev/fuse || ! -w /dev/fuse ]]; then
        echo "/dev/fuse is not readable and writable by $(id -un)" >&2
        ls -l /dev/fuse >&2 || true
        exit 1
    fi

    if ! grep -wq fuse /proc/filesystems; then
        echo "Linux kernel does not report FUSE support in /proc/filesystems" >&2
        exit 1
    fi

    local workdir
    local mounted=0

    workdir="$(mktemp -d)"

    cleanup() {
        if [[ "${mounted}" -eq 1 ]]; then
            fusermount3 -u "${workdir}/merged" >/dev/null 2>&1 || true
        fi
        rm -rf "${workdir}"
    }
    trap cleanup EXIT

    mkdir -p "${workdir}/lower" "${workdir}/upper" "${workdir}/work" "${workdir}/merged"
    printf 'ok\n' > "${workdir}/lower/file"

    fuse-overlayfs \
        -o "lowerdir=${workdir}/lower,upperdir=${workdir}/upper,workdir=${workdir}/work,allow_other" \
        "${workdir}/merged"
    mounted=1

    if [[ "$(cat "${workdir}/merged/file")" != "ok" ]]; then
        echo "FUSE overlay read returned unexpected content" >&2
        exit 1
    fi

    fusermount3 -u "${workdir}/merged"
    mounted=0

    echo "Linux FUSE check passed" >&2
    cleanup
    trap - EXIT
}

inside_shell() {
    if [[ "${1:-}" == "--" ]]; then
        shift
    fi
    exec bash "$@"
}

inside_run_locked() {
    local task="$1"
    shift

    local cache_root="/work/target/container"
    local lock_dir="${cache_root}/cargo.lock.d"

    mkdir -p "${cache_root}" "${CARGO_HOME}" "${CARGO_TARGET_DIR}"

    prepare_dependencies() {
        local recipe_path="${cache_root}/recipe.json"
        local chef_workspace="${cache_root}/chef-workspace"

        cargo chef prepare --recipe-path "${recipe_path}"
        rm -rf "${chef_workspace}"
        mkdir -p "${chef_workspace}"
        (
            cd "${chef_workspace}"
            cargo chef cook \
                --locked \
                --all-targets \
                --recipe-path "${recipe_path}" \
                --target-dir "${CARGO_TARGET_DIR}" \
                --jobs "${CARGO_BUILD_JOBS}"
        )
    }

    run_task() {
        case "${task}" in
            check)
                prepare_dependencies
                cargo check --locked --all-targets --jobs "${CARGO_BUILD_JOBS}"
                ;;
            test)
                prepare_dependencies
                cargo test --locked --tests --jobs "${CARGO_BUILD_JOBS}"
                cargo test --locked --examples --jobs "${CARGO_BUILD_JOBS}"
                cargo test --locked --benches --no-run --jobs "${CARGO_BUILD_JOBS}"
                ;;
            lib)
                prepare_dependencies
                cargo test --locked --lib --jobs "${CARGO_BUILD_JOBS}"
                ;;
            tests)
                prepare_dependencies
                cargo test --locked --tests --jobs "${CARGO_BUILD_JOBS}"
                ;;
            examples)
                prepare_dependencies
                cargo test --locked --examples --jobs "${CARGO_BUILD_JOBS}"
                ;;
            benches)
                prepare_dependencies
                cargo test --locked --benches --no-run --jobs "${CARGO_BUILD_JOBS}"
                ;;
            clippy)
                prepare_dependencies
                cargo clippy --locked --all-targets --all-features --jobs "${CARGO_BUILD_JOBS}" -- -D warnings
                ;;
            cargo)
                if [[ "${1:-}" == "--" ]]; then
                    shift
                fi
                cargo "$@"
                ;;
            *)
                die "unknown locked task: ${task}"
                ;;
        esac
    }

    until mkdir "${lock_dir}" 2>/dev/null; do
        echo "waiting for Cargo cache lock" >&2
        sleep 2
    done
    printf '%s\n' "$$" > "${lock_dir}/pid"
    trap 'rm -rf "${lock_dir}"' EXIT
    run_task "$@"
    rm -rf "${lock_dir}"
    trap - EXIT
}

main() {
    local command="${1:-help}"
    if (($# > 0)); then
        shift
    fi

    case "${command}" in
        help | -h | --help)
            usage
            ;;
        test | check | lib | tests | examples | benches | clippy)
            run_container_task "${command}" "0" "$@"
            ;;
        cargo | shell)
            run_container_task "${command}" "1" "$@"
            ;;
        fmt)
            run_fmt "$@"
            ;;
        build-image)
            run_build_image_command "$@"
            ;;
        __inside-fuse-check)
            require_linux_inside
            inside_fuse_check "$@"
            ;;
        __inside-test)
            require_linux_inside
            inside_run_locked test "$@"
            ;;
        __inside-check)
            require_linux_inside
            inside_run_locked check "$@"
            ;;
        __inside-lib)
            require_linux_inside
            inside_run_locked lib "$@"
            ;;
        __inside-tests)
            require_linux_inside
            inside_run_locked tests "$@"
            ;;
        __inside-examples)
            require_linux_inside
            inside_run_locked examples "$@"
            ;;
        __inside-benches)
            require_linux_inside
            inside_run_locked benches "$@"
            ;;
        __inside-clippy)
            require_linux_inside
            inside_run_locked clippy "$@"
            ;;
        __inside-cargo)
            require_linux_inside
            inside_run_locked cargo "$@"
            ;;
        __inside-shell)
            require_linux_inside
            inside_shell "$@"
            ;;
        *)
            die "unknown command: ${command}"
            ;;
    esac
}

main "$@"
