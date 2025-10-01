#!/bin/bash
# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
set -eo pipefail
# Always run the cleanup script, regardless of the success of bouncing into
# the container.

# TODO: Uncomment after fixing the
# "chmod: changing permissions of [...] Operation not permitted" issue.
# function cleanup() {
#     chmod +x ${KOKORO_GFILE_DIR}/trampoline_cleanup.sh
#     ${KOKORO_GFILE_DIR}/trampoline_cleanup.sh
#     echo "cleanup";
# }
# trap cleanup EXIT

# Install Docker on the cbt-iw machine in order to use the Trampoline script.
sudo apt install -y apt-transport-https ca-certificates curl software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu `lsb_release -cs` test"
sudo apt update
sudo apt install -y docker-ce

# Some VMs don't export KOKORO_ROOT and cause builds to fail. Hence, forcefully
# set it here.
export KOKORO_ROOT="${KOKORO_ROOT:-/tmpfs}"
$(dirname $0)/populate-secrets.sh # Secret Manager secrets.
python3 "${KOKORO_GFILE_DIR}/trampoline_v1.py"
