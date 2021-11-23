package wekafs

import (
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/glog"
	"github.com/google/uuid"
	"github.com/wekafs/csi-wekafs/pkg/wekafs/apiclient"
)

type DirSnapshot struct {
	*csi.Snapshot
	Uid       *uuid.UUID
	apiClient *apiclient.ApiClient
	name string
}

func (s *DirSnapshot) GetType() VolumeType {
	return VolumeTypeDirSnapV1
}

func (s *DirSnapshot) GetId() string {
	return s.SnapshotId
}

func (s *DirSnapshot) getFullPath() string {
	return ""
}

func (s *DirSnapshot) Mount(xattr bool) (error, UnmountFunc) {
	panic("Implement me")
}

func (s *DirSnapshot) Unmount(xattr bool) error {
	panic("Implement me")
}

func (s *DirSnapshot) Exists() (bool, error) {
	panic("Implement me")
}

func (s *DirSnapshot) Create(name string, params *map[string]string) error {
	panic("Implement me")

}

func (s *DirSnapshot) Delete() error {
	glog.V(3).Infoln("Deleting snapshot", s.GetId())
	return nil
}

func (s *DirSnapshot) getSourceVolumeId() string {
	return getVolumeIdFromSnapshotId(s.GetSnapshotId())
}

func (s *DirSnapshot) getCsiSnapshot() *csi.Snapshot {
	return s.Snapshot
}

func (s *DirSnapshot) GetName() string {
	return s.name
}

func (s *DirSnapshot) RefreshApiClient(client *apiclient.ApiClient) {
	glog.V(5).Infoln("Refreshing API client for object")
	s.apiClient = client
}