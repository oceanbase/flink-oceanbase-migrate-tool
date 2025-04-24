#!/usr/bin/env bash
# Copyright 2024 OceanBase.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

##############################################################
# This script is deploy stage jars to repository.apache.org
##############################################################

MVN=${MVN:-mvn}
CUSTOM_OPTIONS=${CUSTOM_OPTIONS:-}

# fail immediately
set -o errexit
set -o nounset

CURR_DIR=`pwd`
###########################


echo "Deploying to repository.apache.org"

echo "Deploying flink 1.15 ..."
${MVN} clean verify gpg:sign install:install deploy:deploy -pl flink-omt -Dgpg.passphrase=$gpgpw -DskipTests=true -Dflink.majorVersion=1.15 -Dflink.version=1.15.1

echo "Deploying flink 1.16 ..."
${MVN} clean verify gpg:sign install:install deploy:deploy -pl flink-omt -Dgpg.passphrase=$gpgpw -DskipTests=true -Dflink.majorVersion=1.16 -Dflink.version=1.16.1

echo "Deploying flink 1.17 ..."
${MVN} clean verify gpg:sign install:install deploy:deploy -pl flink-omt -Dgpg.passphrase=$gpgpw -DskipTests=true -Dflink.majorVersion=1.17 -Dflink.version=1.17.1

echo "Deploying flink 1.18 ..."
${MVN} clean verify gpg:sign install:install deploy:deploy -pl flink-omt -Dgpg.passphrase=$gpgpw -DskipTests=true -Dflink.majorVersion=1.18 -Dflink.version=1.18.1

echo "Deploying flink 1.19 ..."
${MVN} clean verify gpg:sign install:install deploy:deploy -pl flink-omt -Dgpg.passphrase=$gpgpw -DskipTests=true -Dflink.majorVersion=1.19 -Dflink.version=1.19.1

echo "Deploy jar finished."