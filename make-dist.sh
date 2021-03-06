#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

ENGINE=discovery-prep-spark-engine
VER=1.2.0

DEPLOY_NAME=$ENGINE-$VER

mkdir $DEPLOY_NAME
cp $ENGINE/target/$DEPLOY_NAME.jar $DEPLOY_NAME
cp run-prep-spark-engine.sh $DEPLOY_NAME
tar zcvf $DEPLOY_NAME.tar.gz $DEPLOY_NAME

rm -rf $DEPLOY_NAME

#eof
