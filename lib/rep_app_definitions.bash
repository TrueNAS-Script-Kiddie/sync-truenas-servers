#!/usr/bin/bash
# lib/rep_app_definitions.bash
# TrueNAS App-definition replication (plan 06 phase 2): extract → transform → create/update.
#
# Guardrails (plans/06-app-definition-replication.md):
#   - NEVER calls app.delete.
#   - A RUNNING (or otherwise non-STOPPED/CRASHED) target app is skipped with a loud
#     warning — it may be a test-in-progress and must never be clobbered. Opt-in
#     --stop-running-apps stops a RUNNING one for the update and starts it again.
#   - An update is skipped when the catalog versions differ (app.update keeps the target's version).
#   - Manual-only subtask: not part of --subtask=all or the no-subtask default.
#
# midclt app.* payload shapes verified on Goldeye: plans/06-phase0-findings.md.

# Workspace
APP_DEF_TMP_DIR="${SCRIPT_DIR}/../tmp/apps"
mkdir -p "${APP_DEF_TMP_DIR}/json/per_app"
# The extracted configs hold app secrets (DB passwords, tokens): root only
chmod 700 "${APP_DEF_TMP_DIR}" "${APP_DEF_TMP_DIR}/json" "${APP_DEF_TMP_DIR}/json/per_app"

# app.config keys the middleware regenerates on every create/update — never part of the payload
APP_DEF_SERVER_KEYS='del(.ix_context, .ix_certificates, .ix_certificate_authorities, .ix_volumes, .release_name)'

declare SOURCE_ALL_APP_JSON
declare TARGET_ALL_APP_JSON
declare -a APP_DEF_SELECTED_LIST=()

####################################
# Tasks for all apps simultaneously #
####################################

# Extract app.query (both sides) and per-app app.config into JSON files
function Extract_app_definitions() {
    echo "### Extracting app definitions ###"

    local SOURCE_OR_TARGET LOCATION_VAR LOCATION SERVER_ID_VAR SERVER_ID
    local APP_NAME FULL_APP_NAME APP_CONFIG_JSON
    local -a ALL_CONFIGURED_APPS

    # Remove old jsons
    rm -f "${APP_DEF_TMP_DIR}/json/per_app/"*.json 2>/dev/null

    # Selection: apps.json order, filtered by --app (unknown names already rejected at parse time)
    mapfile -t ALL_CONFIGURED_APPS < <(jq -r '.apps[].name' "${SCRIPT_DIR}/../config/apps.json")
    APP_DEF_SELECTED_LIST=()
    for APP_NAME in "${ALL_CONFIGURED_APPS[@]}"; do
        # shellcheck disable=SC2076  # deliberate literal-substring match, not regex
        [[ ${#APP_LIST[@]} -gt 0 && ! " ${APP_LIST[*]} " =~ " ${APP_NAME} " ]] && continue
        APP_DEF_SELECTED_LIST+=( "${APP_NAME}" )
    done

    for SOURCE_OR_TARGET in "SOURCE" "TARGET"; do
        # unset first: 'local -n' can't rebind an existing nameref in some bash.
        unset -n ALL_APP_JSON_REF
        local -n ALL_APP_JSON_REF="${SOURCE_OR_TARGET}_ALL_APP_JSON"

        LOCATION_VAR="${SOURCE_OR_TARGET}_LOCATION"
        LOCATION="${!LOCATION_VAR}"
        SERVER_ID_VAR="${SOURCE_OR_TARGET}_SERVER_ID"
        SERVER_ID="${!SERVER_ID_VAR}"

        # shellcheck disable=SC2034  # nameref write-through to SOURCE_ALL_APP_JSON/TARGET_ALL_APP_JSON
        ALL_APP_JSON_REF="$(Execute_command "${LOCATION}" "midclt call app.query" 2>/dev/null)" \
            || Background_error "ERROR: truenas-${SERVER_ID} - Failed to query app definitions"

        for APP_NAME in "${APP_DEF_SELECTED_LIST[@]}"; do
            FULL_APP_NAME="${APP_NAME}-${SERVER_ID}"
            if ! jq -e --arg N "${FULL_APP_NAME}" '.[] | select(.name==$N)' <<<"${ALL_APP_JSON_REF}" >/dev/null; then
                echo "truenas-${SERVER_ID} - App '${FULL_APP_NAME}' not installed"
                continue
            fi

            APP_CONFIG_JSON="$(Execute_command "${LOCATION}" "midclt call app.config \"${FULL_APP_NAME}\"" 2>/dev/null)" \
                || Background_error "ERROR: truenas-${SERVER_ID} - Failed to get config of '${FULL_APP_NAME}'"
            jq "${APP_DEF_SERVER_KEYS}" <<<"${APP_CONFIG_JSON}" > "${APP_DEF_TMP_DIR}/json/per_app/${APP_NAME}.truenas-${SERVER_ID}.${SOURCE_OR_TARGET,,}.json" \
                || Background_error "ERROR: truenas-${SERVER_ID} - app.config of '${FULL_APP_NAME}' did not return valid JSON"

            echo "truenas-${SERVER_ID} - Extracted ${APP_DEF_TMP_DIR}/json/per_app/${APP_NAME}.truenas-${SERVER_ID}.${SOURCE_OR_TARGET,,}.json"
        done
    done
    unset -n ALL_APP_JSON_REF
    echo
}

# Print an indented diff of two app-config JSON files with secret-looking values masked
# (logs are emailed): equal secrets show as REDACTED, differing ones as REDACTED (before/after).
# Usage: Show_app_config_diff <BEFORE_JSON_FILE> <AFTER_JSON_FILE>
function Show_app_config_diff() {
    local BEFORE_FILE="$1"
    local AFTER_FILE="$2"
    # shellcheck disable=SC2016  # jq program: $-variables are jq's, not bash's
    local MASK_PROGRAM='
        ($A[0]) as $a | ($B[0]) as $b
        | (if $SIDE == "before" then $a else $b end) as $doc
        | reduce ($doc | paths(scalars)) as $p ($doc;
            if ($p[-1] | type) == "string" and ($p[-1] | test("pass|secret|token|key"; "i")) and (getpath($p) | type) == "string"
            then setpath($p; if ($a | getpath($p)) == ($b | getpath($p)) then "REDACTED" else "REDACTED (\($SIDE))" end)
            else . end)'

    diff <(jq -S -n --arg SIDE before --slurpfile A "${BEFORE_FILE}" --slurpfile B "${AFTER_FILE}" "${MASK_PROGRAM}") \
         <(jq -S -n --arg SIDE after  --slurpfile A "${BEFORE_FILE}" --slurpfile B "${AFTER_FILE}" "${MASK_PROGRAM}") \
        | sed 's/^/    /'
}

###############################
# Tasks per app individually  #
###############################

# Transform a source app config into a destination-ready config using
# config/app_config_mappings.json (spec: config/app_divergences.md)
#
# Usage: Transform_app_definition <APP_NAME>
function Transform_app_definition() {
    local APP_NAME="$1"
    local MAP_FILE="${SCRIPT_DIR}/../config/app_config_mappings.json"
    local TARGET_APP_JSON_FILE="${APP_DEF_TMP_DIR}/json/per_app/${APP_NAME}.truenas-${TARGET_SERVER_ID}.target.json"

    echo "Transforming source app config into destination app config..."

    [[ -f "${MAP_FILE}" ]] || Background_error "ERROR: mapping file not found: ${MAP_FILE}"
    # Target not installed (create): keep_target falls back to each rule's default
    [[ -f "${TARGET_APP_JSON_FILE}" ]] || TARGET_APP_JSON_FILE=/dev/null

    # The <app>-master <-> <app>-backup rewrite is the naming convention (see
    # Control_app_with_checks), so it is prepended here rather than configured.
    jq --arg SRC "${SOURCE_SERVER_ID}" --arg TGT "${TARGET_SERVER_ID}" \
       --arg SRC_NAME "${APP_NAME}-${SOURCE_SERVER_ID}" --arg TGT_NAME "${APP_NAME}-${TARGET_SERVER_ID}" \
       --slurpfile MAP "${MAP_FILE}" --slurpfile TGT_CFG "${TARGET_APP_JSON_FILE}" '
        def regex_escape: gsub("(?<c>[.\\[\\]\\\\^$|?*+(){}])"; "\\\(.c)");
        def rewrite($s):
          reduce (
            [{scope: null, ($SRC): $SRC_NAME, ($TGT): $TGT_NAME}]
            + ($MAP[0].string_rewrites.rules // [])
            | .[] | select(.[$SRC] != null and .[$TGT] != null)
          ) as $r (
            $s;
            . as $CUR
            | ($r[$SRC] | tostring) as $FROM
            | ($r[$TGT] | tostring) as $TO
            | if (($r.scope == null) or ($CUR | contains($r.scope))) and ($CUR | contains($FROM))
              # Whole-token only: 192.168.0.10 must not hit 192.168.0.100.
              then ($CUR | gsub("(?<![A-Za-z0-9])" + ($FROM | regex_escape) + "(?![A-Za-z0-9])"; $TO))
              else $CUR
              end
          );
        walk(
          if type == "string" then rewrite(.)
          elif type == "object" then
            reduce (($MAP[0].value_overrides.rules // [])[] | select(.[$SRC] != null and .[$TGT] != null)) as $r (.;
              if has($r.key) and (.[$r.key] == $r[$SRC]) then .[$r.key] = $r[$TGT] else . end)
          else .
          end
        )
        # Per-instance values: the source never decides them. Only paths the source has.
        | reduce ($MAP[0].keep_target.rules // [])[] as $r (.;
            if (try getpath($r.path) catch null) == null then .
            else (try ($TGT_CFG[0] | getpath($r.path)) catch null) as $KEEP
              | setpath($r.path; if $KEEP == null then $r.default else $KEEP end)
            end)
    ' "${SOURCE_APP_JSON_FILE}" > "${TRANSFORMED_APP_JSON_FILE}" \
        || Background_error "ERROR transforming ${SOURCE_APP_JSON_FILE}"

    jq -e . "${TRANSFORMED_APP_JSON_FILE}" >/dev/null \
        || Background_error "ERROR: Transformed JSON failed validation: ${TRANSFORMED_APP_JSON_FILE}"

    echo "  truenas-${LOCAL_SERVER_ID} - Transformed source json into ${TRANSFORMED_APP_JSON_FILE}"
    echo "  Source → target rewrites:"
    Show_app_config_diff "${SOURCE_APP_JSON_FILE}" "${TRANSFORMED_APP_JSON_FILE}"
    echo
}

# Wait for a midclt app job (create/update) to reach SUCCESS. Longer timeout than
# Control_app's 60s: a create pulls container images.
function Wait_for_app_job() {
    local JOBID="$1"
    local LOCATION="$2"
    local DESCRIPTION="$3"

    local JOB_STATE
    local JOB_ERROR
    local TIMEOUT_COUNTER=0
    local MAX_TIMEOUT=600

    while true; do
        JOB_STATE="$(Execute_command "${LOCATION}" "midclt call core.get_jobs \"[[\\\"id\\\",\\\"=\\\",${JOBID}]]\" | jq -r '.[0].state'")"
        [[ "${JOB_STATE}" == "SUCCESS" ]] && break
        if [[ "${JOB_STATE}" =~ ^(FAILED|ABORTED|ERROR)$ ]]; then
            JOB_ERROR="$(Execute_command "${LOCATION}" "midclt call core.get_jobs \"[[\\\"id\\\",\\\"=\\\",${JOBID}]]\" | jq -r '.[0].error // empty'")"
            echo
            Background_error "ERROR: ${DESCRIPTION} ended in state ${JOB_STATE}.${JOB_ERROR:+\n${JOB_ERROR}}"
        fi
        ((TIMEOUT_COUNTER++))
        [[ "${TIMEOUT_COUNTER}" -gt "${MAX_TIMEOUT}" ]] \
            && { echo; Background_error "ERROR: ${DESCRIPTION} did not finish within ${MAX_TIMEOUT}s (job state: ${JOB_STATE})."; }
        echo -n "."
        sleep 1
    done
    echo " Done."
}

# Decide and apply per app: absent -> app.create, present+STOPPED/CRASHED ->
# app.update, otherwise skip with a warning. NEVER app.delete.
# Returns 0 when applied (or test-printed), 1 when skipped.
function Apply_app_definition() {
    local APP_NAME="$1"

    local SOURCE_FULL_NAME="${APP_NAME}-${SOURCE_SERVER_ID}"
    local TARGET_FULL_NAME="${APP_NAME}-${TARGET_SERVER_ID}"
    local TARGET_APP_JSON_FILE="${APP_DEF_TMP_DIR}/json/per_app/${APP_NAME}.truenas-${TARGET_SERVER_ID}.target.json"
    local TARGET_STATE VALUES_JSON PAYLOAD QUOTED_PAYLOAD JOBID POST_CONFIG_FILE
    local TARGET_STOPPED_BY_US="false"

    # Return a target that --stop-running-apps stopped to its previous (running) state
    function l_Restart_target_if_stopped_by_us() {
        [[ "${TARGET_STOPPED_BY_US}" == "true" ]] || return 0
        Start_app_if_stopped "${TARGET_FULL_NAME}" "${TARGET_LOCATION}"
        Unregister_cleanup
    }

    TARGET_STATE="$(jq -r --arg N "${TARGET_FULL_NAME}" '.[] | select(.name==$N) | .state' <<<"${TARGET_ALL_APP_JSON}")"
    VALUES_JSON="$(jq -c . "${TRANSFORMED_APP_JSON_FILE}")"

    # Show what an update would change on the target (also under --test / when skipping)
    if [[ -f "${TARGET_APP_JSON_FILE}" ]]; then
        echo "Target config changes that would be pushed:"
        Show_app_config_diff "${TARGET_APP_JSON_FILE}" "${TRANSFORMED_APP_JSON_FILE}"
        echo
    fi

    if [[ -z "${TARGET_STATE}" || "${TARGET_STATE}" == "null" ]]; then
        # Absent → create with the source's exact catalog coordinates.
        local TRAIN CATALOG_APP VERSION
        TRAIN="$(jq -r --arg N "${SOURCE_FULL_NAME}" '.[] | select(.name==$N) | .metadata.train' <<<"${SOURCE_ALL_APP_JSON}")"
        CATALOG_APP="$(jq -r --arg N "${SOURCE_FULL_NAME}" '.[] | select(.name==$N) | .metadata.name' <<<"${SOURCE_ALL_APP_JSON}")"
        VERSION="$(jq -r --arg N "${SOURCE_FULL_NAME}" '.[] | select(.name==$N) | .version' <<<"${SOURCE_ALL_APP_JSON}")"
        { [[ -n "${TRAIN}" && "${TRAIN}" != "null" && -n "${CATALOG_APP}" && "${CATALOG_APP}" != "null" && -n "${VERSION}" && "${VERSION}" != "null" ]]; } \
            || Background_error "ERROR: Could not determine train/catalog_app/version for '${SOURCE_FULL_NAME}' from app.query (see plans/06-phase0-findings.md item 1)."

        PAYLOAD="$(jq -nc \
            --arg app_name "${TARGET_FULL_NAME}" --arg train "${TRAIN}" \
            --arg catalog_app "${CATALOG_APP}" --arg version "${VERSION}" \
            --argjson values "${VALUES_JSON}" \
            '{app_name: $app_name, train: $train, catalog_app: $catalog_app, version: $version, values: $values}')"

        echo -n "Creating app '${TARGET_FULL_NAME}' (${CATALOG_APP} ${VERSION}, train '${TRAIN}') on truenas-${TARGET_SERVER_ID}...${TEST_MODE:+" (Not done because of '--test' usage!)"}"
        echo
        echo "  truenas-${TARGET_SERVER_ID} - midclt call app.create (values from ${TRANSFORMED_APP_JSON_FILE})"
        [[ -n "${TEST_MODE}" ]] && { echo; return 0; }

        # ' -> '\'' so a quote in a config value can't end the shell argument early
        QUOTED_PAYLOAD=${PAYLOAD//"'"/"'\''"}
        JOBID="$(Execute_command "${TARGET_LOCATION}" "midclt call app.create '${QUOTED_PAYLOAD}'")"
        [[ "${JOBID}" =~ ^[0-9]+$ ]] \
            || Background_error "ERROR: app.create for '${TARGET_FULL_NAME}' did not return a job id. Output: ${JOBID}\nIf version ${VERSION} is not available on truenas-${TARGET_SERVER_ID}, sync/refresh its catalog first — this pipeline never falls back to 'latest'."
        Wait_for_app_job "${JOBID}" "${TARGET_LOCATION}" "app.create of '${TARGET_FULL_NAME}' (if version ${VERSION} is unavailable on the target, sync catalogs first)"

    elif [[ "${TARGET_STATE}" =~ ^(STOPPED|CRASHED)$ || ( "${TARGET_STATE}" == "RUNNING" && -n "${STOP_RUNNING_APPS}" ) ]]; then
        # app.update keeps the target's installed version, and the valid values depend on it.
        local SOURCE_VERSION TARGET_VERSION
        SOURCE_VERSION="$(jq -r --arg N "${SOURCE_FULL_NAME}" '.[] | select(.name==$N) | .version' <<<"${SOURCE_ALL_APP_JSON}")"
        TARGET_VERSION="$(jq -r --arg N "${TARGET_FULL_NAME}" '.[] | select(.name==$N) | .version' <<<"${TARGET_ALL_APP_JSON}")"
        if [[ "${SOURCE_VERSION}" != "${TARGET_VERSION}" ]]; then
            Print_app_version_mismatch "${APP_NAME}" "catalog version" "${SOURCE_VERSION}" "${TARGET_VERSION}"
            return 1
        fi

        if [[ "${TARGET_STATE}" == "RUNNING" ]]; then
            # Register the undo BEFORE stopping, so even a timed-out stop still gets started again.
            # shellcheck disable=SC2016  # single-quoted on purpose: expand at cleanup (fire) time
            Register_cleanup 'Start_app_if_stopped "${TARGET_FULL_NAME}" "${TARGET_LOCATION}"'
            TARGET_STOPPED_BY_US="true"
            echo -n "Stopping ${TARGET_LOCATION} ${TARGET_FULL_NAME} for this run (--stop-running-apps)."
            Control_app "${TARGET_FULL_NAME}" stop "${TARGET_LOCATION}"
        fi

        PAYLOAD="$(jq -nc --argjson values "${VALUES_JSON}" '{values: $values}')"

        echo -n "Updating app '${TARGET_FULL_NAME}' ($([[ "${TARGET_STOPPED_BY_US}" == "true" ]] && echo "was" || echo "state") ${TARGET_STATE}) on truenas-${TARGET_SERVER_ID}...${TEST_MODE:+" (Not done because of '--test' usage!)"}"
        echo
        echo "  truenas-${TARGET_SERVER_ID} - midclt call app.update ${TARGET_FULL_NAME} (values from ${TRANSFORMED_APP_JSON_FILE})"
        [[ -n "${TEST_MODE}" ]] && { l_Restart_target_if_stopped_by_us; echo; return 0; }

        QUOTED_PAYLOAD=${PAYLOAD//"'"/"'\''"}
        JOBID="$(Execute_command "${TARGET_LOCATION}" "midclt call app.update \"${TARGET_FULL_NAME}\" '${QUOTED_PAYLOAD}'")"
        [[ "${JOBID}" =~ ^[0-9]+$ ]] \
            || Background_error "ERROR: app.update for '${TARGET_FULL_NAME}' did not return a job id. Output: ${JOBID}"
        Wait_for_app_job "${JOBID}" "${TARGET_LOCATION}" "app.update of '${TARGET_FULL_NAME}'"

    else
        echo "  !!! WARNING: app '${TARGET_FULL_NAME}' on truenas-${TARGET_SERVER_ID} is ${TARGET_STATE} — definition NOT touched."
        echo "  !!! A running target may be a test-in-progress; stop it there first, or pass --stop-running-apps, if you want its definition synced."
        echo
        return 1
    fi

    # Post-apply check: differences only WARN — server-added defaults are expected
    # noise on the first runs (see plans/06-phase0-findings.md).
    POST_CONFIG_FILE="${APP_DEF_TMP_DIR}/json/per_app/${APP_NAME}.truenas-${TARGET_SERVER_ID}.post_apply.json"
    Execute_command "${TARGET_LOCATION}" "midclt call app.config \"${TARGET_FULL_NAME}\"" 2>/dev/null | jq "${APP_DEF_SERVER_KEYS}" > "${POST_CONFIG_FILE}"
    if ! diff <(jq -S . "${POST_CONFIG_FILE}") <(jq -S . "${TRANSFORMED_APP_JSON_FILE}") >/dev/null 2>&1; then
        echo "  WARNING: target config after apply differs from the transformed definition:"
        Show_app_config_diff "${POST_CONFIG_FILE}" "${TRANSFORMED_APP_JSON_FILE}"
    fi
    l_Restart_target_if_stopped_by_us
    echo
    return 0
}

#######################################
# High-level orchestrator
#######################################

function Perform_app_definition_replication() {
    local APP_NAME
    local SOURCE_FULL_NAME
    local CUSTOM_APP
    local SOURCE_APP_JSON_FILE
    local TRANSFORMED_APP_JSON_FILE

    local SUCCEEDED=0
    local SKIPPED=0

    echo "##############################################"
    echo "### Performing App-definition Replication ###"
    echo "##############################################"
    echo

    Extract_app_definitions

    for APP_NAME in "${APP_DEF_SELECTED_LIST[@]}"; do
        echo "### Replicating app definition: ${APP_NAME} ###"
        echo

        SOURCE_FULL_NAME="${APP_NAME}-${SOURCE_SERVER_ID}"
        SOURCE_APP_JSON_FILE="${APP_DEF_TMP_DIR}/json/per_app/${APP_NAME}.truenas-${SOURCE_SERVER_ID}.source.json"
        TRANSFORMED_APP_JSON_FILE="${APP_DEF_TMP_DIR}/json/per_app/${APP_NAME}.truenas-${TARGET_SERVER_ID}.transformed.json"

        if [[ ! -f "${SOURCE_APP_JSON_FILE}" ]]; then
            echo "App '${SOURCE_FULL_NAME}' is not installed on the source — nothing to replicate."
            echo
            ((SKIPPED++))
            continue
        fi

        CUSTOM_APP="$(jq -r --arg N "${SOURCE_FULL_NAME}" '.[] | select(.name==$N) | .custom_app // false' <<<"${SOURCE_ALL_APP_JSON}")"
        if [[ "${CUSTOM_APP}" == "true" ]]; then
            # Custom apps need a custom_compose_config payload — unsupported until verified.
            echo "  !!! WARNING: '${SOURCE_FULL_NAME}' is a custom app — skipped (compose payload shape unverified, see plans/06-phase0-findings.md)."
            echo
            ((SKIPPED++))
            continue
        fi

        Transform_app_definition "${APP_NAME}"

        if Apply_app_definition "${APP_NAME}"; then
            ((SUCCEEDED++))
        else
            ((SKIPPED++))
        fi
    done

    echo "### App-definition Replication Summary: ${SUCCEEDED} applied, ${SKIPPED} skipped ###"
    echo
}
