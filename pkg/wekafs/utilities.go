package wekafs

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/glog"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/pkg/xattr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

func createVolumeIdFromRequest(req *csi.CreateVolumeRequest, dynamicVolPath string) (string, error) {
	name := req.GetName()

	var volId string
	volType := req.GetParameters()["volumeType"]

	switch volType {
	case "":
		return "", status.Errorf(codes.InvalidArgument, "missing VolumeType in CreateVolumeRequest")

	case string(VolumeTypeDirV1):
		// we have a dir in request or no info
		filesystemName := GetFSNameFromRequest(req)
		if filesystemName == "" {
			return "", status.Errorf(codes.InvalidArgument, "missing filesystem name in CreateVolumeRequest")
		}
		asciiPart := getAsciiPart(name, 64)
		hash := getStringSha1(name)
		folderName := asciiPart + "-" + hash
		if dynamicVolPath != "" {
			volId = filepath.Join(volType, filesystemName, dynamicVolPath, folderName)
		} else {
			volId = filepath.Join(volType, filesystemName, folderName)
		}
		return volId, nil

	case string(VolumeTypeFsV1):
		filesystemName := GetFSNameFromRequest(req)
		if filesystemName != "" {
			glog.Warningln("filesystemName was specified in storage class and is disregarded for volumeType", volType)
		}
		volId = calculateVolumeIdFromFsName(name)
		return volId, nil

	default:
		return "", status.Errorf(codes.InvalidArgument, "Unsupported VolumeType in CreateVolumeRequest")
	}
}

func createSnapshotIdFromRequest(req *csi.CreateSnapshotRequest) (string, error) {
	name := req.GetName()
	sourceVolId := req.GetSourceVolumeId()
	sourceVol, err := NewVolume(sourceVolId, nil, nil, nil)
	if err != nil {
		return "", status.Error(codes.InvalidArgument, err.Error())
	}
	switch sourceVol.GetType() {
	case "":
		return "", status.Errorf(codes.InvalidArgument, "missing sourceVolumeType in CreateVolumeRequest")
	case VolumeTypeDirV1:
		asciiPart := getAsciiPart(name, 64)
		hash := getStringSha1(name)
		snapName := asciiPart + "-" + hash
		ret := string(VolumeTypeDirSnapV1) + "/" + sourceVol.getPartialId() + "/" + snapName
		return ret, nil


	case VolumeTypeFsV1:
		asciiPart := getAsciiPart(name, 20)
		hash := getStringSha1(name)[:11]
		snapName := asciiPart + "-" + hash
		return string(VolumeTypeFsSnapV1) + "/" + sourceVol.getPartialId() + "/" + snapName, nil

	default:
		return "", status.Errorf(codes.InvalidArgument, "Unsupported sourceVolumeType in CreateSnapshotRequest")
	}
}

func getVolumeIdFromSnapshotId(snapshotId string) string {
	var ret string
	if strings.HasPrefix(snapshotId, string(VolumeTypeFsSnapV1)) {
		s := strings.TrimPrefix(snapshotId, string(VolumeTypeFsSnapV1))[1:] // cut the volId
		parts := strings.Split(s, "/")
		ret = string(VolumeTypeFsV1) + "/" + strings.Join(parts[:len(parts)-1], "/")
	}
	if strings.HasPrefix(snapshotId, string(VolumeTypeDirSnapV1)) {
		s := strings.TrimPrefix(snapshotId, string(VolumeTypeDirSnapV1))[1:]

		parts := strings.Split(s, "/")
		ret = string(VolumeTypeDirV1) + "/" + strings.Join(parts[:len(parts)-1], "/")
	}
	return ret
}

func getSnapNameFromSnapshotId(snapshotId string) string {
	parts := strings.Split(snapshotId, "/")
	return parts[len(parts)-1]
}

func calculateVolumeIdFromFsName(name string) string {
	asciiPart := getAsciiPart(name, 20)
	hash := getStringSha1(name)[0:11]
	filesystemName := asciiPart + "-" + hash
	volId := filepath.Join(string(VolumeTypeFsV1), filesystemName)
	return volId
}

func getStringSha1(name string) string {
	h := sha1.New()
	h.Write([]byte(name))
	hash := hex.EncodeToString(h.Sum(nil))
	return hash
}

func GetFSNameFromRequest(req *csi.CreateVolumeRequest) string {
	var filesystemName string
	if val, ok := req.GetParameters()["filesystemName"]; ok {
		// explicitly specified FS name in request
		filesystemName = val
		if filesystemName != "" {
			return filesystemName
		}
	}
	return ""
}

func GetFSName(volumeID string) string {
	// VolID format:
	// "dir/v1/<WEKA_FS_NAME>/<FOLDER_NAME_SHA1_HASH>-<FOLDER_NAME_ASCII>"
	slices := strings.Split(volumeID, "/")
	if len(slices) < 3 {
		return ""
	}
	return slices[2]
}

func GetVolumeDirName(volumeID string) string {
	slices := strings.Split(volumeID, "/")
	if len(slices) < 3 {
		return ""
	}
	return strings.Join(slices[3:], "/") // may be either directory name or innerPath
}

func GetVolumeType(volumeID string) string {
	slices := strings.Split(volumeID, "/")
	if len(slices) >= 2 {
		return strings.Join(slices[0:2], "/")
	}
	return ""
}

func PathExists(p string) bool {
	file, err := os.Open(p)
	if err != nil {
		return false
	}
	defer func() { _ = file.Close() }()

	fi, err := file.Stat()
	if err != nil {
		return false
	}

	if !fi.IsDir() {
		Die("A file was found instead of directory in mount point. Please contact support")
	}
	return true
}

func PathIsWekaMount(path string) bool {
	glog.V(2).Infof("Checking if %s is wekafs mount", path)
	mountcmd := "mount -t wekafs | grep " + path
	res, _ := exec.Command("sh", "-c", mountcmd).Output()
	return strings.Contains(string(res), path)
}

func validateVolumeId(volumeId string) error {
	if len(volumeId) == 0 {
		return status.Errorf(codes.InvalidArgument, "volume ID may not be empty")
	}
	if len(volumeId) > maxVolumeIdLength {
		return status.Errorf(codes.InvalidArgument, "volume ID exceeds max length")
	}

	volumeType := GetVolumeType(volumeId)
	switch volumeType {
	case string(VolumeTypeDirV1):
		// VolID format is as following:
		// "<VolType>/<WEKA_FS_NAME>/<FOLDER_NAME_SHA1_HASH>-<FOLDER_NAME_ASCII>"
		// e.g.
		// "dir/v1/default/63008f52b44ca664dfac8a64f0c17a28e1754213-my-awesome-folder"
		// length limited to maxVolumeIdLength
		r := VolumeTypeDirV1 + "/[^/]*/.+"
		re := regexp.MustCompile(string(r))
		if re.MatchString(volumeId) {
			return nil
		}
	case string(VolumeTypeDirSnapV1):
		// VolID format is as following:
		// "<VolType>/<WEKA_FS_NAME>/<FOLDER_NAME_SHA1_HASH>-<FOLDER_NAME_ASCII>/<SNAPSHOT_NAME_SHA1_HASH>"
		// e.g.
		// "dir/v1/default/63008f52b44ca664dfac8a64f0c17a28e1754213-my-awesome-folder"
		// length limited to maxVolumeIdLength
		r := VolumeTypeDirSnapV1 + "/[^/]*/.+/.+"
		re := regexp.MustCompile(string(r))
		if re.MatchString(volumeId) {
			return nil
		}
	case string(VolumeTypeFsV1):
		// VolID format is as following:
		// "<VolType>/<WEKA_FS_NAME>
		// e.g.
		// "fs/v1/aaaa/
		// length limited to maxVolumeIdLength
		r := VolumeTypeFsV1 + "/[^/]+$"
		re := regexp.MustCompile(string(r))
		if re.MatchString(volumeId) {
			return nil
		}
	case string(VolumeTypeFsSnapV1):
		// VolID format is as following:
		// "<VolType>/<WEKA_FS_NAME>/<WEKA_SNAP_NAME>
		// e.g.
		// "fssnap/v1/aaaa/snapaaaa-aifsda0fyd
		// length limited to maxVolumeIdLength
		r := VolumeTypeFsSnapV1 + "/[^/]+/.+"
		re := regexp.MustCompile(string(r))
		if re.MatchString(volumeId) {
			return nil
		}
	}
	return status.Errorf(codes.InvalidArgument, fmt.Sprintf("unsupported volumeID %s for type %s", volumeId, volumeType))
}

func updateXattrs(volPath string, attrs map[string][]byte) error {
	for key, val := range attrs {
		if err := xattr.Set(volPath, key, val); err != nil {
			return status.Errorf(codes.Internal, "failed to update volume attribute %s: %s, %s", key, val, err.Error())
		}
	}
	glog.V(3).Infof("Xattrs updated on volume: %v", volPath)
	return nil
}

func setVolumeProperties(volPath string, capacity int64, volName string) error {
	// assumes that volPath is already mounted and accessible
	xattrs := make(map[string][]byte)
	if volName != "" {
		xattrs[xattrVolumeName] = []byte(volName)
	}
	if capacity > 0 {
		xattrs[xattrCapacity] = []byte(fmt.Sprint(capacity))
	}
	return updateXattrs(volPath, xattrs)
}

func pathIsDirectory(filename string) error {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return status.Errorf(codes.NotFound, "volume path %s not found", filename)
	}
	if !info.IsDir() {
		return status.Errorf(codes.Internal, "volume path %s is not a valid directory", filename)
	}
	return nil
}

func time2Timestamp(t time.Time) *timestamp.Timestamp {
	return &timestamp.Timestamp{
		Seconds: t.Unix(),
		Nanos: int32(t.Nanosecond()),
	}
}

func Min(x, y int32) int32 {
	if x > y {
		return y
	}
	return x
}