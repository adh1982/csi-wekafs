package wekafs

import (
	"github.com/golang/glog"
	"github.com/wekafs/csi-wekafs/pkg/wekafs/apiclient"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"os/exec"
	"strings"
)

type FsVolume struct {
	id                 string
	Filesystem         string
	GroupName          string
	volumeType         string
	ssdCapacityPercent int
	apiClient          *apiclient.ApiClient
	mountPath          string
	mounter            *wekaMounter
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
		switch t := err.(type) {
		case apiclient.ApiNotFoundError:
			return nil, nil // we know that volume doesn't exist
		default:
			glog.Errorln("Failed to fetch fs object for volume ID", v.GetId(), "filesystem name:", v.Filesystem)
			return nil, t // any other error
		}
	}
	return fs, nil
}

//getSsdCapacity returns the SSD capacity based on required total capacity and storageClass param ssdCapacityPercent
func (v FsVolume) getSsdCapacity(requiredCapacity int64) int64 {
	return requiredCapacity / 100 * int64(v.ssdCapacityPercent/100)
}

func (v FsVolume) UpdateCapacity(capacityLimit int64, params *map[string]string) error {
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
	return v.mountPath
}

func (v FsVolume) GetId() string {
	return v.id
}

func (v FsVolume) isMounted() bool {
	if v.mountPath != "" {
		// good path, we mounted from within this object
		return true
	}
	mountcmd := "mount -t wekafs | grep -w -e '^" + v.Filesystem + " on'"
	res, _ := exec.Command("sh", "-c", mountcmd).Output()
	if len(res) == 0 {
		// could not find a mount that matches current config
		return false
	}
	parts := strings.Split(strings.TrimSpace(string(res)), " ")
	if len(parts) < 3 {
		return false
	}
	v.mountPath = parts[2]
	return true
}

func (v FsVolume) Mount(xattr bool) (error, UnmountFunc) {
	var mountPath string
	var err error
	if !v.isMounted() {
		glog.V(4).Infoln("Volume", v.GetId(), "is not mounted, mounting with xattr:", xattr)
		if xattr {
			mountPath, err, _ = v.mounter.MountXattr(v.Filesystem)
		} else {
			mountPath, err, _ = v.mounter.Mount(v.Filesystem)
		}
		v.mountPath = mountPath
		return err, func() {
			_ = v.Unmount(xattr)
		}
	}
	glog.V(4).Infoln("Volume", v.GetId(), "is already mounted, skipping and setting unmount to nil")
	return nil, func() {}
}

func (v FsVolume) Unmount(xattr bool) error {
	err := v.mounter.Unmount(v.Filesystem)
	if err != nil {
		v.mountPath = ""
	}
	return err
}

func (v FsVolume) Exists() (bool, error) {
	fs, err := v.getObject()
	if err != nil {
		return false, err
	}
	if fs == nil {
		return false, nil
	}

	//if PathIsWekaMount(v.getFullPath(mountPoint)) {
	//	glog.Infof("Volume %s: exists and accessible via %s", v.GetId(), v.getFullPath(mountPoint))
	//	return true, nil
	//}
	//
	//if !PathExists(v.getFullPath(mountPoint)) {
	//	glog.Infof("Volume %s: filesystem %s is not mounted", v.GetId(), v.Filesystem)
	//	return false, nil
	//}
	//if err := pathIsDirectory(v.getFullPath(mountPoint)); err != nil {
	//	glog.Errorf("Volume %s is unusable: path %s is a not a directory", v.GetId(), v.getFullPath(mountPoint))
	//	return false, status.Error(codes.Internal, err.Error())
	//}
	return true, nil
}

func (v FsVolume) createFilesystem(capacity int64) error {
	fs, err := v.getObject()
	if err != nil {
		return status.Error(codes.Internal, "Error fetching existing filesystem")
	}

	if v.GroupName == "" {
		return status.Error(codes.InvalidArgument, "Filesystem group name not specified")
	}

	fsc := &apiclient.FileSystemCreateRequest{
		Name:          v.GetId(),
		GroupName:     v.GroupName,
		TotalCapacity: capacity,
		SsdCapacity:   v.getSsdCapacity(capacity),
		Encrypted:     false,
		AuthRequired:  false,
		AllowNoKms:    false,
	}
	err = v.apiClient.CreateFileSystem(fsc, fs)
	if err != nil {
		glog.Errorln("Failed to create volume", v.GetId(), err)
	}
	return err
}

func (v FsVolume) Create(capacity int64, params *map[string]string) error {
	if err := v.createFilesystem(capacity); err != nil {
		glog.Errorf("Failed to create filesystem %s", v.Filesystem)
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
