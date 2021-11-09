package wekafs

import (
	"errors"
	"fmt"
	"github.com/golang/glog"
	"github.com/wekafs/csi-wekafs/pkg/wekafs/apiclient"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strconv"
)

type FsVolume struct {
	id                  string
	Filesystem          string
	filesystemGroupName string
	volumeType          string
	ssdCapacityPercent  int
	apiClient           *apiclient.ApiClient
	mountPath           map[bool]string
	mounter             *wekaMounter
}

func (v FsVolume) getMaxCapacity() (int64, error) {
	glog.V(5).Infoln("Attempting to get max capacity for a new filesystem", v.Filesystem)
	if v.apiClient == nil {
		return -1, status.Errorf(codes.FailedPrecondition, "Could not bind volume %s to API endpoint", v.Filesystem)
	}
	maxCapacity, err := v.apiClient.GetFreeCapacity()
	if err != nil {
		return -1, status.Errorf(codes.FailedPrecondition, "Could not obtain free capacity for filesystem %s on cluster %s: %s", v.apiClient.ClusterName, err.Error())
	}
	glog.V(5).Infoln("Resolved free capacity as", maxCapacity)
	return int64(maxCapacity), nil
}

func (v FsVolume) GetType() VolumeType {
	return VolumeTypeFsV1
}

func (v FsVolume) GetCapacity() (int64, error) {
	glog.V(3).Infoln("Attempting to get current capacity of volume", v.GetId())
	fs, err := v.getObject()
	if err != nil {
		return -1, err
	}
	size := fs.TotalCapacity
	if size > 0 {
		glog.V(3).Infoln("Current capacity of volume", v.GetId(), "is", size, "obtained via API")
		return size, nil
	}
	return size, nil
}

func (v FsVolume) getObject() (*apiclient.FileSystem, error) {
	if v.apiClient == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "Could not bind volume %s to API endpoint", v.Filesystem)
	}
	fs, err := v.apiClient.GetFileSystemByName(v.Filesystem)
	if err != nil {
		if err == apiclient.ObjectNotFoundError {
			return nil, nil // we know that volume doesn't exist
		}
		glog.Errorln("Failed to fetch fs object for volume ID", v.GetId(), "filesystem name:", v.Filesystem)
		return nil, err
	}
	return fs, nil
}

// getSsdCapacity returns the SSD capacity based on required total capacity and storageClass param ssdCapacityPercent
func (v FsVolume) getSsdCapacity(requiredCapacity int64) int64 {
	if v.ssdCapacityPercent == 100 {
		return requiredCapacity
	}
	return requiredCapacity / 100 * int64(v.ssdCapacityPercent/100)
}

//goland:noinspection GoUnusedParameter
func (v FsVolume) UpdateCapacity(capacityLimit int64, params *map[string]string) error {
	// params are deliberately disregarded in this function
	glog.V(3).Infoln("Updating capacity of the volume", v.GetId(), "to", capacityLimit)

	currentCapacity, err := v.GetCapacity()
	if err != nil {
		return status.Errorf(codes.FailedPrecondition, "Failed to get current volume capacity for volume", v.GetId())
	}

	maxSizeDiff, err := v.getMaxCapacity()
	if err != nil {
		return err
	}
	if currentCapacity+maxSizeDiff < capacityLimit {
		return status.Errorf(codes.FailedPrecondition, "Failed to resize volume as it would exceed free capacity", v.GetId())
	}

	fs, err := v.getObject()
	if err != nil {
		return err
	}

	capLimit := capacityLimit
	ssdLimit := v.getSsdCapacity(capLimit)
	fsu := &apiclient.FileSystemResizeRequest{
		Uid:           fs.Uid,
		TotalCapacity: &capLimit,
		SsdCapacity:   &ssdLimit,
	}

	err = v.apiClient.UpdateFileSystem(fsu, fs)
	if err == nil {
		glog.V(3).Infoln("Successfully updated capacity for volume", v.GetId())
		return err
	}
	glog.Errorln("Failed to set capacity for volume", v.GetId())
	return err
}

func (v FsVolume) moveToTrash() error {
	return nil
}

func (v FsVolume) getFullPath() string {
	return v.mountPath[false]
}

func (v FsVolume) getFullPathXattr() string {
	return v.mountPath[true]
}

func (v FsVolume) GetId() string {
	return v.id
}

func (v FsVolume) isMounted(xattr bool) bool {
	if v.mountPath[xattr] == "" {
		return false
	}
	if xattr {
		if PathIsWekaMount(v.getFullPathXattr()) {
			return true
		}
	} else {
		if PathIsWekaMount(v.getFullPath()) {
			return true
		}
	}
	if v.mounter.HasMount(v.Filesystem, xattr) {
		return true
	}
	return false
}

func (v FsVolume) Mount(xattr bool) (error, UnmountFunc) {
	var mountPath string
	var err error
	if !v.isMounted(xattr) {
		glog.V(4).Infoln("Volume", v.GetId(), "is not mounted, mounting with xattr:", xattr)
		if xattr {
			mountPath, err, _ = v.mounter.MountXattr(v.Filesystem)
		} else {
			mountPath, err, _ = v.mounter.Mount(v.Filesystem)
		}
		v.mountPath[xattr] = mountPath
		return err, func() {
			_ = v.Unmount(xattr)
		}
	}
	glog.V(4).Infoln("Volume", v.GetId(), "is already mounted, skipping and setting unmount to nil")
	return nil, func() {}
}

func (v FsVolume) String() string {
	return fmt.Sprintln("name:", v.Filesystem, ", filesystemGroupName:", v.filesystemGroupName,
		"ID:", v.GetId(), "volumeType:", v.GetType(), "ssdCapacityPercent:", v.ssdCapacityPercent)
}

func (v FsVolume) Unmount(xattr bool) error {
	var err error
	if xattr {
		err = v.mounter.UnmountXattr(v.Filesystem)
	} else {
		err = v.mounter.Unmount(v.Filesystem)
	}
	if err != nil {
		v.mountPath[xattr] = ""
	}
	return err
}

func (v FsVolume) Exists() (bool, error) {
	glog.V(3).Infoln("Checking if volume", v.GetId(), "exists")
	fs, err := v.getObject()
	if err != nil {
		return false, err
	}
	if fs == nil {
		return false, nil
	}

	err, unmount := v.Mount(false)
	defer unmount()
	if err != nil {
		return false, err
	}
	if !PathExists(v.getFullPath()) {
		glog.Infof("Volume %s not found on filesystem %s", v.GetId(), v.Filesystem)
		return false, nil
	}
	if err := pathIsDirectory(v.getFullPath()); err != nil {
		glog.Errorf("Volume %s is unusable: path %s is a not a directory", v.GetId(), v.Filesystem)
		return false, status.Error(codes.Internal, err.Error())
	}
	glog.Infof("Volume %s exists and accessible via %s", v.id, v.getFullPath())
	return true, nil
}

func (v FsVolume) updateValuesFromParams(params *map[string]string) error {
	glog.Infoln("Received the following request params:", createKeyValuePairs(*params))
	if params == nil {
		return errors.New("failed to update filesystem params from request params")
	}
	if val, ok := (*params)["filesystemGroupName"]; ok {
		v.filesystemGroupName = val
		glog.V(5).Infoln("Set filesystemGroupName:", v.String())
	}
	if val, ok := (*params)["ssdCapacityPercent"]; ok {
		ssdPercent, err := strconv.Atoi(val)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "Failed to parse percents from storageclass")
		}
		v.ssdCapacityPercent = ssdPercent
	} else {
		// default value
		v.ssdCapacityPercent = 100
	}
	glog.Infoln("Filesystem object after update:", v)
	return nil
}

func (v FsVolume) Create(capacity int64, params *map[string]string) error {
	if !v.apiClient.SupportsFilesystemAsVolume() {
		return errors.New("volume of type Filesystem is not supported on current version of Weka cluster")
	}

	fs, err := v.getObject()
	if err != nil {
		return err
	}
	if fs != nil {
		// return fs for idempotence // TODO: validate capacity or return error
		return nil
	}

	glog.V(3).Infoln("Filesystem", v.Filesystem, "not found, creating:", v.String())

	err = v.updateValuesFromParams(params)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to fetch volume parameters: %s", err.Error()))
	}

	if v.filesystemGroupName == "" {
		return status.Error(codes.InvalidArgument, "Filesystem group name not specified")
	}

	fsc := &apiclient.FileSystemCreateRequest{
		Name:          v.GetId(),
		GroupName:     v.filesystemGroupName,
		TotalCapacity: capacity,
		SsdCapacity:   v.getSsdCapacity(capacity),
		Encrypted:     false,
		AuthRequired:  false,
		AllowNoKms:    false,
	}
	err = v.apiClient.CreateFileSystem(fsc, fs)
	if err != nil {
		glog.Errorln("Failed to create volume", v.GetId(), err)
		return err
	}
	glog.V(3).Infof("Created volume %s in: %v", v.GetId(), v.Filesystem)
	return nil
}

// Delete is a synchronous delete, used for cleanup on unsuccessful ControllerCreateVolume. GC flow is separate
func (v FsVolume) Delete() error {
	fs, err := v.getObject()
	if err != nil {
		glog.Errorln("Failed to delete filesystem since FS object could not be fetched from API for filesystem", v.Filesystem)
		return status.Errorf(codes.Internal, "Failed to delete filesystem %s", v.Filesystem)
	}
	if fs == nil {
		// FS doesn't exist already, return OK for idempotence
		return nil
	}
	fsd := &apiclient.FileSystemDeleteRequest{Uid: fs.Uid}
	err = v.apiClient.DeleteFileSystem(fsd)
	if err != nil {
		glog.Errorln("Failed to delete filesystem via API", v.Filesystem)
		return status.Errorf(codes.Internal, "Failed to delete filesystem %s: %s", v.Filesystem, err)
	}
	glog.V(4).Infof("Deleted volume %s, mapped to filesystem %s", v.GetId(), v.Filesystem)
	return nil
}
