package wekafs

import (
	"errors"
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
}

func NewVolume(volumeId string, apiClient *apiclient.ApiClient, mounter *wekaMounter, gc *dirVolumeGc) (Volume, error) {
	glog.V(5).Infoln("Creating new volume representation object for volume ID", volumeId)
	if err := validateVolumeId(volumeId); err != nil {
		return &DirVolume{}, err
	}
	if apiClient != nil {
		glog.V(5).Infof("Successfully bound volume to backend API %s@%s", apiClient.Username, apiClient.ClusterName)
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
