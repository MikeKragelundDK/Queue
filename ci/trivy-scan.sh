#!/usr/bin/env bash
# SCA + misconfig + secret-scan af arbejdskopien med Trivy.
# Køres EFTER 'mvn verify', så pom-dependency-resolution sker fra det
# udfyldte ~/.m2 (samme mønster som main-platformens ci/trivy-scan.sh).
# Greenfield-repo: scannet er strict fra dag ét - HIGH/CRITICAL fejler buildet.
set -euo pipefail

TRIVY_VERSION="0.74.0"
BIN_DIR="$(pwd)/.trivy-bin"
REPORT_DIR="$(pwd)/trivy-reports"

if [ ! -x "${BIN_DIR}/trivy" ]; then
  curl -sSfL https://raw.githubusercontent.com/aquasecurity/trivy/main/contrib/install.sh \
    | sh -s -- -b "${BIN_DIR}" "v${TRIVY_VERSION}"
fi

mkdir -p "${REPORT_DIR}"

# --ignore-unfixed: kun sårbarheder med en tilgængelig fix blokerer buildet;
# unfixed kan ikke afhjælpes med en versionsbump og ville bare låse pipelinen.
# target/ skippes: build-output, ikke kildekode.
"${BIN_DIR}/trivy" fs \
  --scanners vuln,misconfig,secret \
  --severity HIGH,CRITICAL \
  --ignore-unfixed \
  --exit-code 1 \
  --skip-dirs "target,.trivy-bin" \
  . | tee "${REPORT_DIR}/trivy-fs-report.txt"
