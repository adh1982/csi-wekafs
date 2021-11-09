package wekafs

import (
	"errors"
	"fmt"
	"github.com/golang/glog"
	"github.com/google/uuid"
	"github.com/pkg/xattr"
	"github.com/wekafs/csi-wekafs/pkg/wekafs/apiclient"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
)

type DirVolume struct {
	id         string
	Filesystem string
	volumeType string
	dirName    string
	apiClient  *apiclient.ApiClient
	mounter    *wekaMounter
	gc         *dirVolumeGc
	mountPath  map[bool]string
}

var ErrNoXattrOnVolume = errors.New("xattr not set on volume")
var ErrBadXattrOnVolume = errors.New("could not parse xattr on volume")

func (v DirVolume) isMounted(xattr bool) bool {
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

func (v DirVolume) getMaxCapacity() (int64, error) {
	glog.V(5).Infoln("Attempting to get max capacity available on filesystem", v.Filesystem)
	err, unmount := v.Mount(true)
	defer unmount()
	if err != nil {
		return 0, err
	}
	var stat syscall.Statfs_t
	err = syscall.Statfs(v.mountPath[true], &stat)
	if err != nil {
		return -1, status.Errorf(codes.FailedPrecondition, "Could not obtain free capacity on mount path %s: %s", v.mountPath, err.Error())
	}
	// Available blocks * size per block = available space in bytes
	maxCapacity := int64(stat.Bavail * uint64(stat.Bsize))
	glog.V(4).Infoln("Max capacity available for a volume:", maxCapacity)
	return maxCapacity, nil
}

func (v DirVolume) GetType() VolumeType {
	return VolumeTypeDirV1
}

func (v DirVolume) GetCapacity() (int64, error) {
	glog.V(3).Infoln("Attempting to get current capacity of volume", v.GetId())
	err, unmount := v.Mount(true)
	defer unmount()
	if err != nil {
		return 0, err
	}
	if v.apiClient != nil && v.apiClient.SupportsQuotaDirectoryAsVolume() {
		size, err := v.getSizeFromQuota()
		if err == nil {
			glog.V(3).Infoln("Current capacity of volume", v.GetId(), "is", size, "obtained via API")
			return int64(size), nil
		}
	}
	glog.V(4).Infoln("Volume", v.GetId(), "appears to be a legacy volume. Trying to fetch capacity from Xattr")
	size, err := v.getSizeFromXattr()
	if err != nil {
		return 0, err
	}
	glog.V(3).Infoln("Current capacity of volume", v.GetId(), "is", size, "obtained via Xattr")
	return int64(size), nil
}

func (v DirVolume) UpdateCapacity(capacityLimit int64, params *map[string]string) error {
	glog.V(3).Infoln("Updating capacity of the volume", v.GetId(), "to", capacityLimit)
	err, unmount := v.Mount(true)
	defer unmount()
	if err != nil {
		return err
	}
	var fallback = true
	enforceCapacity, err := getStrictCapacityFromParams(*params)

	f := func() error { return v.updateCapacityQuota(&enforceCapacity, capacityLimit) }
	if v.apiClient == nil {
		glog.V(4).Infof("Volume has no API client bound, updating capacity in legacy mode")
		f = func() error { return v.updateCapacityXattr(&enforceCapacity, capacityLimit) }
		fallback = false
	} else if !v.apiClient.SupportsQuotaDirectoryAsVolume() {
		glog.V(4).Infoln("Updating quota via API not supported by Weka cluster, updating capacity in legacy mode")
		f = func() error { return v.updateCapacityXattr(&enforceCapacity, capacityLimit) }
		fallback = false
	}
	err = f()
	if err == nil {
		glog.V(3).Infoln("Successfully updated capacity for volume", v.GetId())
		return err
	}
	if fallback {
		glog.V(4).Infoln("Failed to set quota for a volume, maybe it is not empty? Falling back to xattr")
		f = func() error { return v.updateCapacityXattr(&enforceCapacity, capacityLimit) }
		err := f()
		if err == nil {
			glog.V(3).Infoln("Successfully updated capacity for volume in FALLBACK mode", v.GetId())
		} else {
			glog.Errorln("Failed to set capacity for quota volume", v.GetId(), "even in fallback")
		}
		return err
	}
	return err
}

func (v DirVolume) updateCapacityQuota(enforceCapacity *bool, capacityLimit int64) error {
	if enforceCapacity != nil {
		glog.V(4).Infoln("Updating quota on volume", v.GetId(), "to", capacityLimit, "enforceCapacity:", *enforceCapacity)
	} else {
		glog.V(4).Infoln("Updating quota on volume", v.GetId(), "to", capacityLimit, "enforceCapacity:", "RETAIN")
	}
	inodeId, err := v.getInodeId()
	if err != nil {
		glog.Errorln("Failed to fetch inode ID for volume", v.GetId())
		return err
	}
	fs, err := v.getFilesystemObj()
	if err != nil {
		glog.Errorln("Failed to fetch filesystem for volume", v.GetId())
		return err
	}

	// check if the quota already exists. If not - create it and exit
	q, err := v.apiClient.GetQuotaByFileSystemAndInode(fs, inodeId)
	if err != nil {
		if err != apiclient.ObjectNotFoundError {
			// any other error
			glog.Errorln("Failed to get quota:", err)
			return status.Error(codes.Internal, err.Error())
		}
		_, err := v.CreateQuotaFromMountPath(enforceCapacity, uint64(capacityLimit))
		return err
	}

	var quotaType apiclient.QuotaType
	if enforceCapacity != nil {
		if !*enforceCapacity {
			quotaType = apiclient.QuotaTypeSoft
		} else {
			quotaType = apiclient.QuotaTypeHard
		}
	} else {
		quotaType = apiclient.QuotaTypeDefault
	}

	if q.GetQuotaType() == quotaType && q.GetCapacityLimit() == uint64(capacityLimit) {
		glog.V(4).Infoln("No need to update quota as it matches current")
		return nil
	}
	updatedQuota := &apiclient.Quota{}
	r := apiclient.NewQuotaUpdateRequest(*fs, inodeId, quotaType, uint64(capacityLimit))
	glog.V(5).Infoln("Constructed update request", r.String())
	err = v.apiClient.UpdateQuota(r, updatedQuota)
	if err != nil {
		glog.V(4).Infoln("Failed to set quota on volume", v.GetId(), "to", updatedQuota.GetQuotaType(), updatedQuota.GetCapacityLimit(), err)
		return err
	}
	glog.V(4).Infoln("Successfully set quota on volume", v.GetId(), "to", updatedQuota.GetQuotaType(), updatedQuota.GetCapacityLimit())
	return nil
}

func (v DirVolume) updateCapacityXattr(enforceCapacity *bool, capacityLimit int64) error {
	glog.V(4).Infoln("Updating xattrs on volume", v.GetId(), "to", capacityLimit, "enforce capacity:", *enforceCapacity)
	if enforceCapacity != nil && *enforceCapacity {
		glog.V(3).Infof("Legacy volume does not support enforce capacity")
	}
	err := setVolumeProperties(v.getFullPathXattr(), capacityLimit, v.dirName)
	if err != nil {
		glog.Errorln("Failed to update xattrs on volume", v.GetId(), "capacity is not set")
	}
	return err
}

func (v DirVolume) moveToTrash() error {
	err, unmount := v.Mount(false)
	defer unmount()
	if err != nil {
		glog.Errorf("Error mounting %s for deletion %s", v.id, err)
		return err
	}
	garbageFullPath := filepath.Join(v.mountPath[false], garbagePath)
	glog.Infof("Ensuring that garbagePath %s exists", garbageFullPath)
	err = os.MkdirAll(garbageFullPath, DefaultVolumePermissions)
	if err != nil {
		glog.Errorf("Failed to create garbagePath %s", garbageFullPath)
		return err
	}
	u, _ := uuid.NewUUID()
	volumeTrashLoc := filepath.Join(garbageFullPath, u.String())
	glog.Infof("Attempting to move volume %s %s -> %s", v.id, v.getFullPath(), volumeTrashLoc)
	if err = os.Rename(v.getFullPath(), volumeTrashLoc); err == nil {
		v.dirName = u.String()
		if v.gc != nil {
			v.gc.triggerGcVolume(v) // TODO: Better to preserve immutability some way , needed due to recreation of volumes with same name
			glog.V(4).Infof("Moved %s to trash", v.id)
			return err
		}
		glog.V(3).Infoln("Volume has no GC bound, cannot trash it")
		return err
	} else {
		glog.V(4).Infof("Failed moving %s to trash: %s", v.getFullPath(), err)
		return err
	}
}

func (v DirVolume) getFullPathXattr() string {
	if v.mountPath[true] == "" {
		return ""
	}
	return filepath.Join(v.mountPath[true], v.dirName)
}

func (v DirVolume) getFullPath() string {
	if v.mountPath[false] == "" {
		return ""
	}
	return filepath.Join(v.mountPath[false], v.dirName)
}

//getInodeId used for obtaining the mount Path inode ID (to set quota on it later)
func (v DirVolume) getInodeId() (uint64, error) {
	err, unmount := v.Mount(false)
	defer unmount()
	if err != nil {
		return 0, err
	}
	glog.V(5).Infoln("Getting inode ID of volume", v.GetId(), "fullpath: ", v.getFullPath())
	fileInfo, err := os.Stat(v.getFullPath())
	if err != nil {
		glog.Error(err)
		return 0, err
	}
	stat, ok := fileInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, errors.New(fmt.Sprintf("failed to obtain inodeId from %s", v.mountPath))
	}
	glog.V(5).Infoln("Inode ID of the volume", v.GetId(), "is", stat.Ino)
	return stat.Ino, nil
}

func (v DirVolume) GetId() string {
	return v.id
}

func (v DirVolume) CreateQuotaFromMountPath(enforceCapacity *bool, capacityLimit uint64) (*apiclient.Quota, error) {
	var quotaType apiclient.QuotaType
	if enforceCapacity != nil {
		if !*enforceCapacity {
			quotaType = apiclient.QuotaTypeSoft
		} else {
			quotaType = apiclient.QuotaTypeHard
		}
	} else {
		quotaType = apiclient.QuotaTypeDefault
	}
	glog.V(4).Infoln("Creating a quota for volume", v.GetId(), "capacity limit:", capacityLimit, "quota type:", quotaType)

	fs, err := v.getFilesystemObj()
	if err != nil {
		return nil, err
	}
	inodeId, err := v.getInodeId()
	if err != nil {
		return nil, errors.New("cannot set quota, could not find inode ID of the volume")
	}
	qr := apiclient.NewQuotaCreateRequest(*fs, inodeId, quotaType, capacityLimit)
	q := &apiclient.Quota{}
	if err := v.apiClient.CreateQuota(qr, q, true); err != nil {
		return nil, err
	}
	glog.V(4).Infoln("Quota successfully set for volume", v.GetId())
	return q, nil
}

func (v DirVolume) getQuota() (*apiclient.Quota, error) {
	glog.V(4).Infoln("Getting existing quota for volume", v.GetId())
	fs, err := v.getFilesystemObj()
	if err != nil {
		return nil, err
	}
	inodeId, err := v.getInodeId()
	if err != nil {
		return nil, err
	}
	ret, err := v.apiClient.GetQuotaByFileSystemAndInode(fs, inodeId)
	if ret != nil {
		glog.V(4).Infoln("Successfully acquired existing quota for volume", v.GetId(), ret.GetQuotaType(), ret.GetCapacityLimit())
	}
	return ret, err
}

func (v DirVolume) getSizeFromQuota() (uint64, error) {
	q, err := v.getQuota()
	if err != nil {
		return 0, err
	}
	if q != nil {
		return q.GetCapacityLimit(), nil
	}
	return 0, errors.New("could not fetch quota from API")
}

func (v DirVolume) getSizeFromXattr() (uint64, error) {
	if capacityString, err := xattr.Get(v.getFullPathXattr(), xattrCapacity); err == nil {
		if capacity, err := strconv.ParseInt(string(capacityString), 10, 64); err == nil {
			return uint64(capacity), nil
		}
		return 0, ErrBadXattrOnVolume
	}
	return 0, ErrNoXattrOnVolume
}

func (v DirVolume) getFilesystemObj() (*apiclient.FileSystem, error) {
	fs, err := v.apiClient.GetFileSystemByName(v.Filesystem)
	if err != nil {
		return nil, err
	}
	return fs, nil
}

func (v DirVolume) Mount(xattr bool) (error, UnmountFunc) {
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

func (v DirVolume) Unmount(xattr bool) error {
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

func (v DirVolume) Exists() (bool, error) {
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

func (v DirVolume) Create(capacity int64, params *map[string]string) error {
	err, unmount := v.Mount(true)
	defer unmount()
	if err != nil {
		return err
	}
	volPath := v.getFullPathXattr()
	if err := os.MkdirAll(volPath, DefaultVolumePermissions); err != nil {
		glog.Errorf("Failed to create directory %s", volPath)
		return err
	}

	glog.Infof("Created directory %s, updating its capacity to %v", v.getFullPathXattr(), capacity)
	// Update volume metadata on directory using xattrs
	if err := v.UpdateCapacity(capacity, params); err != nil {
		glog.Warningf("Failed to update capacity on newly created volume %s in: %s, deleting", v.id, volPath)
		err2 := v.Delete()
		if err2 != nil {
			glog.V(2).Infof("Failed to clean up directory %s after unsuccessful set capacity", v.dirName)
		}
		return err
	}
	glog.V(3).Infof("Created volume %s in: %v", v.id, volPath)
	return nil
}

// Delete is a synchronous delete, used for cleanup on unsuccessful ControllerCreateVolume. GC flow is separate
func (v DirVolume) Delete() error {
	glog.V(4).Infof("Deleting volume %s, located in filesystem %s", v.id, v.Filesystem)
	err, unmount := v.Mount(true)
	defer unmount()
	if err != nil {
		return err
	}
	volPath := v.getFullPath()
	_ = os.RemoveAll(volPath)
	glog.V(2).Infof("Deleted volume %s in :%v", v.id, volPath)
	return nil
}
