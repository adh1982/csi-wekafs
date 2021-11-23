/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package wekafs

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/glog"
	"github.com/wekafs/csi-wekafs/pkg/wekafs/apiclient"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strconv"
	"strings"
	"sync"
)

const (
	deviceID          = "deviceID"
	maxVolumeIdLength = 1920
)

type controllerServer struct {
	caps           []*csi.ControllerServiceCapability
	nodeID         string
	gc             *dirVolumeGc
	mounter        *wekaMounter
	creatLock      sync.Mutex
	dynamicVolPath string
	api            *apiStore
	snapshots      *SnapshotDB
}

//goland:noinspection GoUnusedParameter
func (cs *controllerServer) ControllerPublishVolume(c context.Context, request *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	panic("implement me")
}

//goland:noinspection GoUnusedParameter
func (cs *controllerServer) ControllerUnpublishVolume(c context.Context, request *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	panic("implement me")
}

//goland:noinspection GoUnusedParameter
func (cs *controllerServer) ListVolumes(c context.Context, request *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	panic("implement me")
}

//goland:noinspection GoUnusedParameter
func (cs *controllerServer) GetCapacity(c context.Context, request *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	panic("implement me")
}

//goland:noinspection GoUnusedParameter
//func (cs *controllerServer) CreateSnapshot(c context.Context, request *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
//	panic("implement me")
//}

//goland:noinspection GoUnusedParameter
//func (cs *controllerServer) DeleteSnapshotRecord(c context.Context, request *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
//	panic("implement me")
//}

//goland:noinspection GoUnusedParameter
func (cs *controllerServer) ControllerGetVolume(context.Context, *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	panic("implement me")
}

func NewControllerServer(nodeID string, api *apiStore, mounter *wekaMounter, gc *dirVolumeGc, dynamicVolPath string) *controllerServer {
	return &controllerServer{
		caps: getControllerServiceCapabilities(
			[]csi.ControllerServiceCapability_RPC_Type{
				csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
				csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
				csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
				csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
			}),
		nodeID:         nodeID,
		mounter:        mounter,
		gc:             gc,
		dynamicVolPath: dynamicVolPath,
		api:            api,
		snapshots:      NewSnapshotDB(),
	}
}

func createKeyValuePairs(m map[string]string) string {
	b := new(bytes.Buffer)
	for key, value := range m {
		_, _ = fmt.Fprintf(b, "%s=\"%s\"\n", key, value)
	}
	return b.String()
}

func CreateVolumeError(errorCode codes.Code, errorMessage string) (*csi.CreateVolumeResponse, error) {
	glog.ErrorDepth(1, fmt.Sprintln("Error creating volume, code:", errorCode, ", error:", errorMessage))
	err := status.Error(errorCode, strings.ToLower(errorMessage))
	return &csi.CreateVolumeResponse{}, err
}

//goland:noinspection GoUnusedParameter
func (cs *controllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	glog.V(3).Infof(">>>> Received a CreateVolume request: %s (%s)", req.GetName(), createKeyValuePairs(req.GetParameters()))
	defer glog.V(3).Infof("<<<< Completed processing CreateVolume request: %s (%s)", req.GetName(), createKeyValuePairs(req.GetParameters()))
	cs.creatLock.Lock()
	defer cs.creatLock.Unlock()
	if err := cs.validateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		glog.V(3).Infof("invalid create volume req: %v", req)
		return nil, err
	}

	// Check arguments
	if len(req.GetName()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Name missing in request")
	}
	caps := req.GetVolumeCapabilities()
	if caps == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capabilities missing in request")
	}

	// Need to calculate volumeID first thing due to possible mapping to multiple FSes
	volumeID, err := createVolumeIdFromRequest(req, cs.dynamicVolPath)
	if err != nil {
		return CreateVolumeError(codes.InvalidArgument, fmt.Sprintln("Failed to resolve VolumeType from CreateVolumeRequest", err))
	}
	// Validate access type in request
	for _, capability := range caps {
		if capability.GetBlock() != nil {
			return CreateVolumeError(codes.InvalidArgument, "Block accessType is unsupported")
		}
	}

	// Check if volume should be created from source
	cSource := req.GetVolumeContentSource()
	if cSource != nil {
		glog.V(3).Infoln("Requested to create a volume from content source", cSource.GetType())
		if cSource.GetVolume() != nil {
			return CreateVolumeError(codes.Unimplemented, "Not supported creating volume from volume")
		}
		if cSource.GetSnapshot() != nil {
			glog.V(3).Infoln("Asking to create a volume from snapshot")
			snapId := cSource.GetSnapshot().SnapshotId
			existingSnap := cs.snapshots.GetSnapRecordById(snapId)
			if existingSnap == nil {
				return CreateVolumeError(codes.NotFound, fmt.Sprintln("Could not find snapshot with ID", snapId, "to create a new volume from"))
			}
		}
	}
	// obtain client for volume
	client, err := cs.api.GetClientFromSecrets(req.Secrets)
	if err != nil {
		return CreateVolumeError(codes.Internal, fmt.Sprintln("Failed to initialize Weka API client for the request", err))
	}
	volume, err := NewVolume(volumeID, client, cs.mounter, cs.gc)

	if err != nil {
		return CreateVolumeError(codes.Internal, err.Error())
	}

	if volume.GetType() == VolumeTypeFsV1 && client == nil {
		return CreateVolumeError(codes.InvalidArgument, "Cannot provision volume of type Filesystem without API")
	}

	// Check for maximum available capacity
	capacity := req.GetCapacityRange().GetRequiredBytes()

	// If directory already exists, return the create response for idempotence if size matches, or error
	volExists, err := volume.Exists()
	if err != nil {
		return CreateVolumeError(codes.Internal, fmt.Sprintln("Could not check if volume exists, volumeID", volume.GetId()))
	}
	if volExists {
		glog.V(3).Infoln("Volume already exists having ID", volume.GetId(), "checking capacity")
		currentCapacity, err := volume.GetCapacity()
		if err != nil {
			return CreateVolumeError(codes.Internal, err.Error())
		}
		// TODO: Once we have everything working - review this, big potential of race of several CreateVolume requests
		if currentCapacity != capacity && currentCapacity != 0 {
			return CreateVolumeError(codes.AlreadyExists,
				fmt.Sprintf("Volume with same ID exists with different capacity volumeID %s: [current]%d!=%d[requested]",
					volumeID, currentCapacity, capacity))
		}
		glog.V(3).Infoln("Volume already exists having ID", volume.GetId(), "and matches capacity, assuming repeating request")
		return &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId:      volumeID,
				CapacityBytes: req.GetCapacityRange().GetRequiredBytes(),
				VolumeContext: req.GetParameters(),
			},
		}, nil
	}

	// validate minimum capacity before create new volume
	glog.V(3).Infoln("Validating enough capacity on storage for creating the volume")
	maxStorageCapacity, err := volume.getMaxCapacity()
	if err != nil {
		return CreateVolumeError(codes.Internal, fmt.Sprintf("CreateVolume: Cannot obtain free capacity for volume %s", volumeID))
	}
	if capacity > maxStorageCapacity {
		return CreateVolumeError(codes.OutOfRange, fmt.Sprintf("Requested capacity %d exceeds maximum allowed %d", capacity, maxStorageCapacity))
	}

	// Actually try to create the volume here
	params := req.GetParameters()
	glog.V(3).Infoln("Creating filesystem", volume.getPartialId(), "capacity:", capacity, "params:", params)
	if err := volume.Create(capacity, &params); err != nil {
		return CreateVolumeError(codes.Internal, err.Error())
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeID,
			CapacityBytes: req.GetCapacityRange().GetRequiredBytes(),
			VolumeContext: req.GetParameters(),
		},
	}, nil
}

func getStrictCapacityFromParams(params map[string]string) (bool, error) {
	qt := ""
	if val, ok := params["capacityEnforcement"]; ok {
		qt = val
	}
	enforceCapacity := true
	switch apiclient.QuotaType(qt) {
	case apiclient.QuotaTypeSoft:
		enforceCapacity = false
	case apiclient.QuotaTypeHard:
		enforceCapacity = true
	case "":
		enforceCapacity = true
	default:
		glog.Warningf("Could not recognize capacity enforcement in params: %s", qt)
		return false, errors.New("unsupported capacityEnforcement in volume params")
	}
	return enforceCapacity, nil
}

func DeleteVolumeError(errorCode codes.Code, errorMessage string) (*csi.DeleteVolumeResponse, error) {
	glog.ErrorDepth(1, fmt.Sprintln("Error deleting volume, code:", errorCode, ", error:", errorMessage))
	err := status.Error(errorCode, strings.ToLower(errorMessage))
	return &csi.DeleteVolumeResponse{}, err
}

//goland:noinspection GoUnusedParameter
func (cs *controllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	glog.V(3).Infof(">>>> Received a DeleteVolume request for volume ID %s", req.GetVolumeId())
	defer glog.V(3).Infof("<<<< Completed processing DeleteVolume request for volume ID %s", req.GetVolumeId())
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	client, err := cs.api.GetClientFromSecrets(req.Secrets)
	if err != nil {
		return DeleteVolumeError(codes.Internal, fmt.Sprintln("Failed to initialize Weka API client for the request", err))
	}

	volume, err := NewVolume(volumeID, client, cs.mounter, cs.gc)
	if err != nil {
		// Should return ok on incorrect ID (by CSI spec)
		return &csi.DeleteVolumeResponse{}, nil
	}

	if err := cs.validateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		glog.V(3).Infof("invalid delete volume req: %v", req)
		return DeleteVolumeError(codes.Internal, err.Error())
	}

	exists, err := volume.Exists()
	if err != nil {
		glog.V(4).Infof("Failed to check for existence of volume %s, but returning success for idempotence", volume.GetId())
		return &csi.DeleteVolumeResponse{}, nil
	}
	if !exists {
		glog.V(4).Infof("Volume not found %s, returning success for idempotence", volume.GetId())
		return &csi.DeleteVolumeResponse{}, nil
	}

	err = volume.moveToTrash()
	if err != nil {
		return DeleteVolumeError(codes.Internal, err.Error())
	}
	return &csi.DeleteVolumeResponse{}, nil
}

func ExpandVolumeError(errorCode codes.Code, errorMessage string) (*csi.ControllerExpandVolumeResponse, error) {
	glog.ErrorDepth(1, fmt.Sprintln("Error expanding volume, code:", errorCode, ", error:", errorMessage))
	err := status.Error(errorCode, strings.ToLower(errorMessage))
	return &csi.ControllerExpandVolumeResponse{}, err
}

//goland:noinspection GoUnusedParameter
func (cs *controllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	glog.V(3).Infof(">>>> Received a ControllerExpandVolume request for volume ID %s", req.GetVolumeId())
	defer glog.V(3).Infof("<<<< Completed processing ControllerExpandVolume request for volume ID %s", req.GetVolumeId())

	if len(req.GetVolumeId()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Volume ID not specified")
	}
	client, err := cs.api.GetClientFromSecrets(req.Secrets)
	if err != nil {
		return ExpandVolumeError(codes.Internal, fmt.Sprintln("Failed to initialize Weka API client for the request", err))
	}

	volume, err := NewVolume(req.GetVolumeId(), client, cs.mounter, cs.gc)
	if err != nil {
		return ExpandVolumeError(codes.NotFound, fmt.Sprintf("Volume with id %s does not exist", req.GetVolumeId()))
	}

	capRange := req.GetCapacityRange()
	if capRange == nil {
		return ExpandVolumeError(codes.InvalidArgument, "Capacity range not provided")
	}

	capacity := capRange.GetRequiredBytes()

	maxStorageCapacity, err := volume.getMaxCapacity()
	if err != nil {
		return ExpandVolumeError(codes.Unknown, fmt.Sprintf("ExpandVolume: Cannot obtain free capacity for volume %s", volume.GetId()))
	}
	if capacity > maxStorageCapacity {
		return ExpandVolumeError(codes.OutOfRange, fmt.Sprintf("Requested capacity %d exceeds maximum allowed %d", capacity, maxStorageCapacity))
	}

	ok, err := volume.Exists()
	if err != nil {
		return ExpandVolumeError(codes.Internal, err.Error())
	}
	if !ok {
		return ExpandVolumeError(codes.Internal, "Volume does not exist")
	}

	currentSize, err := volume.GetCapacity()
	if err != nil {
		return ExpandVolumeError(codes.Internal, "Could not get volume capacity")
	}
	glog.Infof("Volume %s: current capacity: %d, expanding to %d", volume.GetId(), currentSize, capacity)

	if currentSize != capacity {
		params := make(map[string]string)
		if err := volume.UpdateCapacity(capacity, &params); err != nil {
			return ExpandVolumeError(codes.Internal, fmt.Sprintf("Could not update volume %s: %v", volume, err))
		}
	}
	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         capacity,
		NodeExpansionRequired: false, // since this is filesystem, no need to resize on node
	}, nil
}

func CreateSnapshotError(errorCode codes.Code, errorMessage string) (*csi.CreateSnapshotResponse, error) {
	glog.ErrorDepth(1, fmt.Sprintln("Error creating snapshot, code:", errorCode, ", error:", errorMessage))
	err := status.Error(errorCode, strings.ToLower(errorMessage))
	return &csi.CreateSnapshotResponse{}, err
}

//goland:noinspection GoUnusedParameter
func (cs *controllerServer) CreateSnapshot(c context.Context, request *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	volumeId := request.GetSourceVolumeId()
	secrets := request.GetSecrets()
	name := request.GetName()
	params := request.GetParameters()
	glog.V(3).Infof(">>>> Received a CreateSnapshot request for volume ID %s, name: %s", volumeId, name)
	defer glog.V(3).Infof("<<<< Completed processing CreateSnapshot request for volume ID %s, name: %s", volumeId, name)

	if name == "" {
		return CreateSnapshotError(codes.InvalidArgument, "Cannot create snapshot without name")
	}

	client, err := cs.api.GetClientFromSecrets(secrets)
	if err != nil {
		return CreateSnapshotError(codes.Internal, fmt.Sprintln("Failed to initialize Weka API client for the request", err))
	}

	volume, err := NewVolume(volumeId, client, cs.mounter, nil)
	if err != nil {
		return CreateSnapshotError(codes.InvalidArgument, fmt.Sprintln("Invalid sourceVolumeId", volumeId))
	}
	//if volume.GetType() != VolumeTypeFsV1 {
	//	return CreateSnapshotError(codes.Unimplemented, fmt.Sprintln("Snapshots are not supported for volume type", volume.GetType()))
	//}

	err = validateVolumeId(volumeId)
	if err != nil {
		return CreateSnapshotError(codes.InvalidArgument, fmt.Sprintln("Failed to valdiate sourceVolumeId", err))
	}

	snapId, err := createSnapshotIdFromRequest(request)
	if err != nil {
		return CreateSnapshotError(codes.InvalidArgument, fmt.Sprintln("Failed to valdiate volumeId", err))
	}
	if snapId == "" {
		return CreateSnapshotError(codes.InvalidArgument, fmt.Sprintln("Failed to valdiate snapId", err))
	}
	// validate source volume ID is new or same as before
	snapName := getSnapNameFromSnapshotId(snapId)
	existingSnap := cs.snapshots.GetSnapRecordByName(snapName)
	if existingSnap != nil {
		if existingSnap.getSourceVolumeId() != volumeId {
			glog.V(4).Infoln("Found existing snapshot", existingSnap.GetName(), "for source volume ID", existingSnap.getSourceVolumeId())
			glog.V(4).Infoln("Requesting to create snapshot", existingSnap.GetName(), "for source volume ID", volumeId)
			// trying to create a snapshot with same existing name but different volume ID
			return CreateSnapshotError(codes.AlreadyExists, fmt.Sprintln("Cannot create snapshot with same name but different volId"))
		} else {
			// return OK for idempotence
			glog.V(4).Infoln("Existing snapshot matches the CreateSnapshot request, skipping")
			ret := &csi.CreateSnapshotResponse{
				Snapshot: existingSnap.getCsiSnapshot(),
			}
			return ret, nil
		}
	}

	glog.Infoln("Creating snapshot name", snapName, "snapId", snapId, "params", params)
	s, err := volume.CreateSnapshot(snapName, snapId, params)
	if err != nil {
		return CreateSnapshotError(codes.Internal, err.Error())
	}

	err = cs.snapshots.InsertSnapshotRecord(s)
	glog.Errorln(s)
	if err != nil {
		panic("Snapshot could not be stored in DB")
	}

	ret := &csi.CreateSnapshotResponse{
		Snapshot: s.getCsiSnapshot(),
	}
	return ret, nil
}

func DeleteSnapshotError(errorCode codes.Code, errorMessage string) (*csi.DeleteSnapshotResponse, error) {
	glog.ErrorDepth(1, fmt.Sprintln("Error deleting snapshot, code:", errorCode, ", error:", errorMessage))
	err := status.Error(errorCode, strings.ToLower(errorMessage))
	return &csi.DeleteSnapshotResponse{}, err
}

//goland:noinspection GoUnusedParameter
func (cs *controllerServer) DeleteSnapshot(c context.Context, request *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	snapshotID := request.GetSnapshotId()
	secrets := request.GetSecrets()
	glog.V(3).Infoln(">>>> Received a DeleteSnapshot request for snapshotId", snapshotID, secrets)
	defer glog.V(3).Infoln("<<<< Completed processing DeleteSnapshot request for snapshotId", snapshotID)
	if snapshotID == "" {
		return DeleteSnapshotError(codes.InvalidArgument, "Failed to delete snapshot, no ID specified")
	}
	err := validateVolumeId(snapshotID)
	if err != nil {
		//according to CSI specs must return OK on invalid ID
		return &csi.DeleteSnapshotResponse{}, nil
	}

	//unknown snap just remove silently
	snap := cs.snapshots.GetSnapRecordById(snapshotID)
	if snap == nil {
		glog.Warningln("Failed to find snapshot by ID", snapshotID, "still removing!")
		return &csi.DeleteSnapshotResponse{}, nil
	}

	client, err := cs.api.GetClientFromSecrets(secrets)
	if err != nil {
		return DeleteSnapshotError(codes.Internal, fmt.Sprintln("Failed to initialize Weka API client for the request", err))
	}
	if client != nil {
		snap.RefreshApiClient(client)
	}

	err = snap.Delete()
	if err != nil {
		return DeleteSnapshotError(codes.Internal, fmt.Sprintln("Failed to delete snapshot", snapshotID, err))
	}
	err = cs.snapshots.DeleteSnapshotRecord(snap)
	if err != nil {
		panic("Failed to remove snapshot from DB")
	}
	return &csi.DeleteSnapshotResponse{}, err
}

func ListSnapshotsError(errorCode codes.Code, errorMessage string) (*csi.ListSnapshotsResponse, error) {
	glog.ErrorDepth(1, fmt.Sprintln("Error expanding volume, code:", errorCode, ", error:", errorMessage))
	err := status.Error(errorCode, strings.ToLower(errorMessage))
	return &csi.ListSnapshotsResponse{}, err
}

//goland:noinspection GoUnusedParameter
func (cs *controllerServer) ListSnapshots(c context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	sourceVolumeId := req.GetSourceVolumeId()
	SnapshotId := req.GetSnapshotId()
	maxEntries := req.GetMaxEntries()
	startToken := req.GetStartingToken()

	glog.V(3).Infof(">>>> Received a ListSnapshots request for volume ID %s, snapshot ID %s, max entries %s", sourceVolumeId, SnapshotId, maxEntries)
	defer glog.V(3).Infof("<<<< Completed processing ListSnapshots request for volume ID %s, snapshot ID %s", sourceVolumeId, SnapshotId)
	var snaps []Snapshot

	// trivial case of snapshotId
	if SnapshotId != "" {
		snap := cs.snapshots.GetSnapRecordById(SnapshotId)
		if snap != nil {
			snaps = append(snaps, snap)
		}
	} else {
		// otherwise bring the whole list
		snaps = cs.snapshots.GetEntries()
	}
	if sourceVolumeId != "" {
		glog.Infoln("Pruning snapshot list by sourceVolumeId", sourceVolumeId)
		var filtered []Snapshot
		for _, s := range snaps {
			if s.getSourceVolumeId() == sourceVolumeId {
				filtered = append(filtered, s)
			}
		}
		snaps = filtered
	}
	glog.V(3).Infoln("Total", len(snaps), "snapshots match criteria")

	var startFrom int32 = 0
	if startToken != "" {
		t, _ := strconv.ParseInt(startToken, 10, 32)
		startFrom = int32(t)
	}
	last := int32(len(snaps))
	if maxEntries > 0 {
		last = Min(startFrom+maxEntries, int32(len(snaps)))
	}

	entries := &[]*csi.ListSnapshotsResponse_Entry{}
	for _, s := range (snaps)[startFrom:last] {
		*entries = append(*entries, &csi.ListSnapshotsResponse_Entry{Snapshot: s.getCsiSnapshot()})
	}
	nextToken := ""
	if last < int32(len(snaps)) {
		nextToken = strconv.FormatInt(int64(last), 10)
	}

	return &csi.ListSnapshotsResponse{
		Entries:   *entries,
		NextToken: nextToken,
	}, nil
}

//goland:noinspection GoUnusedParameter
func (cs *controllerServer) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: cs.caps,
	}, nil
}

func ValidateVolumeCapsError(errorCode codes.Code, errorMessage string) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	glog.ErrorDepth(1, fmt.Sprintln("Error getting volume capabilities, code:", errorCode, ", error:", errorMessage))
	err := status.Error(errorCode, strings.ToLower(errorMessage))
	return &csi.ValidateVolumeCapabilitiesResponse{}, err
}

//goland:noinspection GoUnusedParameter
func (cs *controllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	glog.V(3).Infof(">>>> Received a ValidateVolumeCapabilities request for volume ID %s", req.GetVolumeId())
	defer glog.V(3).Infof("<<<< Completed processing ValidateVolumeCapabilities request for volume ID %s", req.GetVolumeId())

	// Check arguments
	if len(req.GetVolumeId()) == 0 {
		return ValidateVolumeCapsError(codes.InvalidArgument, "Volume ID cannot be empty")
	}
	if len(req.GetVolumeCapabilities()) == 0 {
		return nil, status.Error(codes.InvalidArgument, req.GetVolumeId())
	}
	// this part must be added to make sure we return NotExists rather than Invalid
	if err := validateVolumeId(req.GetVolumeId()); err != nil {
		return ValidateVolumeCapsError(codes.NotFound, fmt.Sprintf("Volume ID %s not found", req.GetVolumeId()))

	}
	client, err := cs.api.GetClientFromSecrets(req.Secrets)
	if err != nil {
		return ValidateVolumeCapsError(codes.Internal, fmt.Sprintln("Failed to initialize Weka API client for the request", err))
	}

	volume, err := NewVolume(req.GetVolumeId(), client, cs.mounter, cs.gc)
	if err != nil {
		return ValidateVolumeCapsError(codes.Internal, err.Error())
	}
	// TODO: Mount/validate in xattr if there is anything to validate. Right now mounting just to see if folder exists
	if ok, err2 := volume.Exists(); err2 != nil && ok {
		return ValidateVolumeCapsError(codes.NotFound, fmt.Sprintf("Could not find volume %s", req.GetVolumeId()))
	}

	for _, capability := range req.GetVolumeCapabilities() {
		if capability.GetMount() == nil && capability.GetBlock() == nil {
			return ValidateVolumeCapsError(codes.InvalidArgument, "cannot have both Mount and block access type be undefined")
		}
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeContext:      req.GetVolumeContext(),
			VolumeCapabilities: req.GetVolumeCapabilities(),
			Parameters:         req.GetParameters(),
		},
	}, nil
}

func (cs *controllerServer) validateControllerServiceRequest(c csi.ControllerServiceCapability_RPC_Type) error {
	if c == csi.ControllerServiceCapability_RPC_UNKNOWN {
		return nil
	}

	for _, capability := range cs.caps {
		if c == capability.GetRpc().GetType() {
			return nil
		}
	}
	return status.Errorf(codes.InvalidArgument, "unsupported capability %s", c)
}

func getControllerServiceCapabilities(cl []csi.ControllerServiceCapability_RPC_Type) []*csi.ControllerServiceCapability {
	var csc []*csi.ControllerServiceCapability

	for _, capability := range cl {
		glog.Infof("Enabling controller service capability: %v", capability.String())
		csc = append(csc, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: capability,
				},
			},
		})
	}

	return csc
}
