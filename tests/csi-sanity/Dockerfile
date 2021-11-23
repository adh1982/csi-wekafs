FROM golang
#ENV test=test
RUN GO_ENABLED=0 GOOS=linux GOARCH=amd64 go get -u -t github.com/kubernetes-csi/csi-test/cmd/csi-sanity || true
ADD wekafs-dirv1.yaml /test/wekafs-dirv1.yaml
ADD wekafs-existingPathv1.yaml /test/wekafs-existingPathv1.yaml
ADD wekafs-fsv1.yaml /test/wekafs-fsv1.yaml
ADD wekafs-fsv1-secret.yaml /test/wekafs-fsv1-secret.yaml
ADD _docker_run_sanity.sh /usr/bin/run_sanity
ADD .tmp-bin/plugin_linux /usr/local/bin/wekafsplugin
