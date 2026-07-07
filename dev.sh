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
  ./dev.sh bench [--build] [--image IMAGE] [--cpus N] [--memory SIZE] [-- <criterion args...>]
  ./dev.sh bench-profile [--build] [--image IMAGE] [--cpus N] [--memory SIZE] [-- <profile args...>]
  ./dev.sh clippy [--build] [--image IMAGE] [--cpus N] [--memory SIZE]
  ./dev.sh cargo [--build] [--image IMAGE] [--cpus N] [--memory SIZE] -- <args...>
  ./dev.sh shell [--build] [--image IMAGE] [--cpus N] [--memory SIZE] -- <args...>
  ./dev.sh fmt
  ./dev.sh build-image [--image IMAGE] [--cpus N] [--memory SIZE]
  ./dev.sh help
USAGE
}

bench_profile_usage() {
    cat <<'USAGE'
Usage:
  ./dev.sh bench-profile [--build] [--image IMAGE] [--cpus N] [--memory SIZE] [-- <profile args...>]

Profile args:
  --profile-time SECONDS    Seconds to run each selected benchmark. Default: 10
  --filter REGEX            Select benchmark IDs matching REGEX. Default: .*
  --target eventfs|host|both
                            Select benchmark target. Default: eventfs
  --perf                    Also collect Linux perf data for each selected benchmark
  --perf-event EVENT        perf event to sample. Default: cpu-clock
  --list                    Print selected benchmark IDs and exit
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

parse_bench_profile_options() {
    PROFILE_TIME="10"
    PROFILE_FILTER=".*"
    PROFILE_TARGET="eventfs"
    PROFILE_PERF="0"
    PROFILE_PERF_EVENT="cpu-clock"
    PROFILE_LIST="0"

    if [[ "${1:-}" == "--" ]]; then
        shift
    fi

    while (($# > 0)); do
        case "$1" in
            --profile-time)
                require_option_value "$1" "${2:-}"
                PROFILE_TIME="$2"
                shift 2
                ;;
            --profile-time=*)
                PROFILE_TIME="${1#*=}"
                require_option_value "--profile-time" "${PROFILE_TIME}"
                shift
                ;;
            --filter)
                require_option_value "$1" "${2:-}"
                PROFILE_FILTER="$2"
                shift 2
                ;;
            --filter=*)
                PROFILE_FILTER="${1#*=}"
                require_option_value "--filter" "${PROFILE_FILTER}"
                shift
                ;;
            --target)
                require_option_value "$1" "${2:-}"
                PROFILE_TARGET="$2"
                shift 2
                ;;
            --target=*)
                PROFILE_TARGET="${1#*=}"
                require_option_value "--target" "${PROFILE_TARGET}"
                shift
                ;;
            --perf)
                PROFILE_PERF="1"
                shift
                ;;
            --perf-event)
                require_option_value "$1" "${2:-}"
                PROFILE_PERF_EVENT="$2"
                shift 2
                ;;
            --perf-event=*)
                PROFILE_PERF_EVENT="${1#*=}"
                require_option_value "--perf-event" "${PROFILE_PERF_EVENT}"
                shift
                ;;
            --list)
                PROFILE_LIST="1"
                shift
                ;;
            -h | --help)
                bench_profile_usage
                exit 0
                ;;
            *)
                die "unexpected argument for bench-profile: $1"
                ;;
        esac
    done

    if [[ ! "${PROFILE_TIME}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
        die "invalid profile time: ${PROFILE_TIME}"
    fi
    case "${PROFILE_TARGET}" in
        eventfs | host | both)
            ;;
        *)
            die "invalid profile target: ${PROFILE_TARGET}"
            ;;
    esac
}

profile_filter_matches() {
    local benchmark_id="$1"

    if [[ "${benchmark_id}" =~ ${PROFILE_FILTER} ]]; then
        return 0
    fi
    local status="$?"
    if [[ "${status}" -eq 2 ]]; then
        die "invalid --filter regex: ${PROFILE_FILTER}"
    fi
    return 1
}

selected_benchmark_ids() {
    local operations=(
        lookup
        getattr
        setattr_metadata
        setattr_size
        access
        statfs
        mknod
        mkdir
        create
        unlink
        rmdir
        rename
        rename_noreplace
        link
        symlink
        readlink
        open
        read
        write
        flush
        release
        fsync
        opendir
        readdir
        readdirplus
        fsyncdir
        releasedir
        setxattr
        getxattr
        listxattr
        removexattr
    )
    local targets=()
    local operation
    local target
    local benchmark_id

    case "${PROFILE_TARGET}" in
        eventfs)
            targets=(eventfs)
            ;;
        host)
            targets=(host)
            ;;
        both)
            targets=(eventfs host)
            ;;
    esac

    for operation in "${operations[@]}"; do
        for target in "${targets[@]}"; do
            benchmark_id="fuse_operations/${operation}/${target}"
            if profile_filter_matches "${benchmark_id}"; then
                printf '%s\n' "${benchmark_id}"
            fi
        done
    done
}

profile_rustflags() {
    local flags="-C force-frame-pointers=yes -C symbol-mangling-version=v0"
    if [[ -n "${RUSTFLAGS:-}" ]]; then
        printf '%s %s' "${RUSTFLAGS}" "${flags}"
    else
        printf '%s' "${flags}"
    fi
}

run_profiled_benchmark() {
    local benchmark_id="$1"
    local rustflags="$2"

    echo "profiling ${benchmark_id} for ${PROFILE_TIME}s" >&2
    RUSTFLAGS="${rustflags}" cargo bench \
        --locked \
        --bench fuse_operations \
        --jobs "${CARGO_BUILD_JOBS}" \
        -- \
        "${benchmark_id}" \
        --exact \
        --profile-time "${PROFILE_TIME}"
}

require_perf_ready() {
    if ! command -v perf >/dev/null 2>&1; then
        die "perf is unavailable in the development image; rebuild with ./dev.sh build-image or rerun bench-profile with --build"
    fi
    if ! perf stat -e "${PROFILE_PERF_EVENT}" -- true >/dev/null 2>&1; then
        die "perf event is unavailable in this container: ${PROFILE_PERF_EVENT}"
    fi
}

run_perf_profiled_benchmark() {
    local benchmark_id="$1"
    local rustflags="$2"
    local output_root="$3"
    local group
    local operation
    local target
    local output_dir

    IFS=/ read -r group operation target <<<"${benchmark_id}"
    output_dir="${output_root}/${operation}/${target}"
    mkdir -p "${output_dir}"

    echo "profiling ${benchmark_id} with perf event ${PROFILE_PERF_EVENT} for ${PROFILE_TIME}s" >&2
    RUSTFLAGS="${rustflags}" perf record \
        -o "${output_dir}/perf.data" \
        -e "${PROFILE_PERF_EVENT}" \
        -F 997 \
        --call-graph fp \
        -- \
        cargo bench \
        --locked \
        --bench fuse_operations \
        --jobs "${CARGO_BUILD_JOBS}" \
        -- \
        "${benchmark_id}" \
        --exact \
        --profile-time "${PROFILE_TIME}"

    perf report -i "${output_dir}/perf.data" --stdio >"${output_dir}/perf-report.txt"
    perf script -i "${output_dir}/perf.data" >"${output_dir}/perf-script.txt"
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

    run_bench_profile_task() {
        parse_bench_profile_options "$@"

        local benchmark_ids=()
        local benchmark_id
        while IFS= read -r benchmark_id; do
            benchmark_ids+=("${benchmark_id}")
        done < <(selected_benchmark_ids)

        if [[ "${#benchmark_ids[@]}" -eq 0 ]]; then
            die "no benchmark IDs matched --filter ${PROFILE_FILTER} and --target ${PROFILE_TARGET}"
        fi

        if [[ "${PROFILE_LIST}" == "1" ]]; then
            printf '%s\n' "${benchmark_ids[@]}"
            return
        fi

        prepare_dependencies

        local rustflags
        rustflags="$(profile_rustflags)"

        if [[ "${PROFILE_PERF}" == "1" ]]; then
            require_perf_ready
            local run_id
            local output_root
            run_id="$(date -u +%Y%m%dT%H%M%SZ)"
            output_root="/work/target/container/profiles/fuse_operations/${run_id}"
            mkdir -p "${output_root}"
            echo "perf profile output: ${output_root}" >&2
            for benchmark_id in "${benchmark_ids[@]}"; do
                run_perf_profiled_benchmark "${benchmark_id}" "${rustflags}" "${output_root}"
            done
        else
            for benchmark_id in "${benchmark_ids[@]}"; do
                run_profiled_benchmark "${benchmark_id}" "${rustflags}"
            done
        fi
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
            bench)
                prepare_dependencies
                if (($# > 0)); then
                    cargo bench --locked --bench fuse_operations --jobs "${CARGO_BUILD_JOBS}" -- "$@"
                else
                    cargo bench --locked --bench fuse_operations --jobs "${CARGO_BUILD_JOBS}"
                fi
                ;;
            bench-profile)
                run_bench_profile_task "$@"
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
    trap "rm -rf \"${lock_dir}\"" EXIT
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
        bench | bench-profile)
            run_container_task "${command}" "1" "$@"
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
        __inside-bench)
            require_linux_inside
            inside_run_locked bench "$@"
            ;;
        __inside-bench-profile)
            require_linux_inside
            inside_run_locked bench-profile "$@"
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
