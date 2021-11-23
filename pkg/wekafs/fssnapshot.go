package wekafs

import (
	"errors"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/glog"
	"github.com/google/uuid"
	"github.com/wekafs/csi-wekafs/pkg/wekafs/apiclient"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

type FsSnapshot struct {
	*csi.Snapshot
	Uid       *uuid.UUID
	apiClient *apiclient.ApiClient
	name string
}

func (s *FsSnapshot) GetType() VolumeType {
	return VolumeTypeFsSnapV1
}

func (s *FsSnapshot) GetId() string {
	return s.SnapshotId
}

func (s *FsSnapshot) getFullPath() string {
	return ""
}

func (s *FsSnapshot) Mount(xattr bool) (error, UnmountFunc) {
	panic("Implement me")
}

func (s *FsSnapshot) Unmount(xattr bool) error {
	panic("Implement me")
}

func (s *FsSnapshot) Exists() (bool, error) {
	panic("Implement me")
}

func (s *FsSnapshot) Create(name string, params *map[string]string) error {
	panic("Implement me")
}

func (s *FsSnapshot) getObject() (*apiclient.Snapshot, error) {
	if s.apiClient == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "Could not bind snapshot %s to API endpoint", s.Snapshot)
	}
	snap := &apiclient.Snapshot{}
	err := s.apiClient.GetSnapshotByUid(*s.Uid, snap)
	if err != nil {
		if err == apiclient.ObjectNotFoundError {
			return &apiclient.Snapshot{}, nil // we know that volume doesn't exist
		}
		glog.Errorln("Failed to fetch snap object for snapshot ID", s.GetId())
		return &apiclient.Snapshot{}, err
	}
	return snap, nil
}

func (s *FsSnapshot) Delete() error {
	glog.V(3).Infoln("Deleting snapshot", s.GetId())
	snapd := &apiclient.SnapshotDeleteRequest{Uid: *s.Uid}
	err := s.apiClient.DeleteSnapshot(snapd)
	if err != nil {
		glog.Errorln("Failed to perform Delete on snapshot", err)
		if err == apiclient.ObjectNotFoundError {
			return nil
		}
		return err
	}
	// we need to wait till it is deleted
	for start := time.Now(); time.Since(start) < time.Second * 30; {
		snap, err := s.getObject()
		if err != nil {
			if err == apiclient.ObjectNotFoundError {
				return nil
			}
		}
		if snap.Uid == uuid.Nil {
			glog.V(5).Infoln("Snapshot", s.GetName(), "was deleted successfully")
			return nil
		} else if ! snap.IsRemoving {
			return errors.New("Snapshot was not marked for deletion although should")
		} else {
			glog.V(4).Infoln("Snapshot is still deleting on system")
		}
		time.Sleep(time.Second)
	}
	return errors.New("Failed to remove snapshot on time after 30 seconds")

}

func (s *FsSnapshot) getSourceVolumeId() string {
	return getVolumeIdFromSnapshotId(s.GetSnapshotId())
}

func (s *FsSnapshot) getCsiSnapshot() *csi.Snapshot {
	return s.Snapshot
}

func (s *FsSnapshot) GetName() string {
	return s.name
}

func (s *FsSnapshot) RefreshApiClient(client *apiclient.ApiClient) {
	glog.V(5).Infoln("Refreshing API client for object")
	s.apiClient = client
}