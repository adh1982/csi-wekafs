package wekafs

import (
	"errors"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/glog"
	"github.com/wekafs/csi-wekafs/pkg/wekafs/apiclient"
)

type Volume interface {
	GetType() VolumeType
	GetId() string
	moveToTrash() error
	getFullPath() string
	UpdateCapacity(capacity int64, params *map[string]string) error
	GetCapacity() (int64, error)
	Mount(xattr bool) (error, UnmountFunc)
	Unmount(xattr bool) error
	Exists() (bool, error)
	getMaxCapacity() (int64, error)
	Create(capacity int64, params *map[string]string) error
	Delete() error
	CreateSnapshot(name string, snapId string, params map[string]string) (Snapshot, error)
	getPartialId() string
}

func NewVolume(volumeId string, apiClient *apiclient.ApiClient, mounter *wekaMounter, gc *dirVolumeGc) (Volume, error) {
	glog.V(5).Infoln("Creating new volume representation object for volume ID", volumeId)
	if err := validateVolumeId(volumeId); err != nil {
		return &DirVolume{}, err
	}
	if apiClient != nil {
		glog.V(5).Infoln("Successfully bound volume to backend API", apiClient.Credentials)
	} else {
		glog.V(5).Infof("Volume was not bound to any backend API client")
	}
	volumeType := GetVolumeType(volumeId)
	switch volumeType {
	case string(VolumeTypeDirV1):
		return &DirVolume{
			id:         volumeId,
			Filesystem: GetFSName(volumeId),
			volumeType: GetVolumeType(volumeId),
			dirName:    GetVolumeDirName(volumeId),
			apiClient:  apiClient,
			mounter:    mounter,
			gc:         gc,
			mountPath:  make(map[bool]string),
		}, nil
	case string(VolumeTypeFsV1):
		return &FsVolume{
			id:                  volumeId,
			Filesystem:          GetFSName(volumeId),
			volumeType:          GetVolumeType(volumeId),
			ssdCapacityPercent:  100,
			apiClient:           apiClient,
			mounter:             mounter,
			mountPath:           make(map[bool]string),
			filesystemGroupName: "",
		}, nil
	default:
		return nil, errors.New("unsupported volume type requested")
	}
}

type Snapshot interface {
	GetName() string
	GetType() VolumeType
	GetId() string
	getFullPath() string
	Mount(xattr bool) (error, UnmountFunc)
	Unmount(xattr bool) error
	Exists() (bool, error)
	Create(name string, params *map[string]string) error
	Delete() error
	getSourceVolumeId() string
	getCsiSnapshot() *csi.Snapshot
	RefreshApiClient(client *apiclient.ApiClient)
}
