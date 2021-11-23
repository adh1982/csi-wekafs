#!/usr/bin/env bash
set -e

rm -rf /tmp/weka-csi-test/sanity-workspace/
rm -rf /tmp/weka-csi-test/filesystems

csi-sanity -csi.stagingdir /tmp/csi-test-staging \
  --csi.controllerendpoint /tmp/weka-csi-test/controller.sock \
  --csi.endpoint /tmp/weka-csi-test/node.sock \
  -csi.mountdir=/tmp/weka-csi-test/sanity-workspace/ \
  -ginkgo.failFast \
  -ginkgo.progress \
  -ginkgo.seed $RANDOM \
  -ginkgo.v \
  -csi.testvolumeparameters /test/wekafs-dirv1.yaml

#
##mkdir -p  /tmp/weka-csi-test/filesystems/default/test/my/path
#
csi-sanity -csi.stagingdir /tmp/csi-test-staging \
  --csi.controllerendpoint /tmp/weka-csi-test/controller.sock \
  --csi.endpoint /tmp/weka-csi-test/node.sock \
  -csi.secrets /test/wekafs-fsv1-secret.yaml \
  -csi.mountdir=/tmp/weka-csi-test/sanity-workspace/ \
  -ginkgo.failFast \
  -ginkgo.progress \
  -ginkgo.seed $RANDOM \
  -ginkgo.v \
  -csi.testvolumeparameters /test/wekafs-fsv1.yaml \
#  -ginkgo.focus ListSnapshots
