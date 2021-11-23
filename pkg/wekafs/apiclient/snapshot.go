package apiclient

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"k8s.io/helm/pkg/urlutil"
	"time"
)

type Snapshot struct {
	IsWritable   bool      `json:"isWritable"`
	FilesystemId string    `json:"filesystemId"`
	Filesystem   string    `json:"filesystem,omitempty"`
	Locator      string    `json:"locator"`
	IsRemoving   bool      `json:"isRemoving"`
	Name         string    `json:"name"`
	Progress     int       `json:"progress"`
	Uid          uuid.UUID `json:"uid"`
	AccessPoint  string    `json:"accessPoint"`
	StowStatus   string    `json:"stowStatus"`
	MultiObs     bool      `json:"multiObs"`
	Type         string    `json:"type"`
	CreationTime time.Time `json:"creationTime"`
	Id           string    `json:"id"`
}

func (snap *Snapshot) String() string {
	return fmt.Sprintln("Snapshot(snapUid:", snap.Uid, "name:", snap.Name, "writable:", snap.IsWritable, "locator:", snap.Locator, "created on:", snap.CreationTime)
}

// FindSnapshotsByFilter returns result set of 0-many objects matching filter
func (a *ApiClient) FindSnapshotsByFilter(query *Snapshot, resultSet *[]Snapshot) error {
	ret := &[]Snapshot{}
	err := a.Get(query.GetBasePath(), nil, ret)
	if err != nil {
		return err
	}
	for _, r := range *ret {
		if r.EQ(query) {
			*resultSet = append(*resultSet, r)
		}
	}
	return nil
}

func (a *ApiClient) FindSnapshotsByFilesystem(query *FileSystem, resultSet *[]Snapshot) error {
	if query == nil {
		return errors.New("cannot search for snapshots without filesystem object")
	}
	snapQuery := &Snapshot{
		FilesystemId: query.Id,
		Name:         query.Name,
	}
	return a.FindSnapshotsByFilter(snapQuery, resultSet)
}

// GetSnapshotByFilter expected to return exactly one result of FindSnapshotsByFilter (error)
func (a *ApiClient) GetSnapshotByFilter(query *Snapshot) (*Snapshot, error) {
	rs := &[]Snapshot{}
	err := a.FindSnapshotsByFilter(query, rs)
	if err != nil {
		return &Snapshot{}, err
	}
	if *rs == nil || len(*rs) == 0 {
		return &Snapshot{}, ObjectNotFoundError
	}
	if len(*rs) > 1 {
		return &Snapshot{}, MultipleObjectsFoundError
	}
	result := &(*rs)[0]
	return result, nil
}

func (a *ApiClient) GetSnapshotByName(name string) (*Snapshot, error) {
	query := &Snapshot{Name: name}
	return a.GetSnapshotByFilter(query)
}

func (a *ApiClient) GetSnapshotByUid(uid uuid.UUID, snap *Snapshot) error {
	ret := &Snapshot{
		Uid: uid,
	}
	return a.Get(ret.GetApiUrl(), nil, snap)
}

func (a *ApiClient) CreateSnapshot(r *SnapshotCreateRequest, snap *Snapshot) error {
	f := a.Log(3, "Creating snapshot", r)
	defer f()
	if !r.hasRequiredFields() {
		return RequestMissingParams
	}
	payload, err := json.Marshal(r)
	if err != nil {
		return err
	}

	err = a.Post(r.getRelatedObject().GetBasePath(), &payload, nil, snap)
	if err != nil {
		return err
	}
	return nil
}

func (a *ApiClient) UpdateSnapshot(r *SnapshotUpdateRequest, snap *Snapshot) error {
	f := a.Log(3, "Updating snapshot", r)
	defer f()
	if !r.hasRequiredFields() {
		return RequestMissingParams
	}
	var payload []byte
	payload, err := json.Marshal(r)
	if err != nil {
		return err
	}
	err = a.Put(r.getApiUrl(), &payload, nil, snap)
	if err != nil {
		return err
	}
	return nil
}

func (a *ApiClient) DeleteSnapshot(r *SnapshotDeleteRequest) error {
	f := a.Log(3, "Deleting snapshot", r)
	defer f()
	if !r.hasRequiredFields() {
		return RequestMissingParams
	}
	apiResponse := &ApiResponse{}
	err := a.Delete(r.getApiUrl(), nil, nil, apiResponse)
	if err != nil {
		switch err.(type) {
		case ApiNotFoundError:
			a.Log(3, "Snapshot", r.Uid, "was deleted successfully")
			return ObjectNotFoundError
		}
	}
	return err
}

func (snap *Snapshot) GetType() string {
	return "snapshot"
}

func (snap *Snapshot) GetBasePath() string {
	return "snapshots"
}

func (snap *Snapshot) GetApiUrl() string {
	url, err := urlutil.URLJoin(snap.GetBasePath(), snap.Uid.String())
	if err != nil {
		return ""
	}
	return url
}

func (snap *Snapshot) getImmutableFields() []string {
	return []string{
		"Name",
		"FilesystemId",
	}
}

func (snap *Snapshot) EQ(q ApiObject) bool {
	return ObjectsAreEqual(q, snap)
}

type SnapshotCreateRequest struct {
	FsUid         uuid.UUID  `json:"fs_uid"`
	Name          string     `json:"name"`
	AccessPoint   string     `json:"access_point"`
	SourceSnapUid *uuid.UUID `json:"source_snap_uid,omitempty"`
	IsWritable    bool       `json:"is_writable,omitempty"`
}

func (snapc *SnapshotCreateRequest) getApiUrl() string {
	return snapc.getRelatedObject().GetBasePath()
}

func (snapc *SnapshotCreateRequest) getRequiredFields() []string {
	return []string{"Name", "FsUid"}
}

func (snapc *SnapshotCreateRequest) hasRequiredFields() bool {
	return ObjectRequestHasRequiredFields(snapc)
}

func (snapc *SnapshotCreateRequest) getRelatedObject() ApiObject {
	return &Snapshot{}
}

func (snapc *SnapshotCreateRequest) String() string {
	return fmt.Sprintln("Snapshot(name:", snapc.Name, "access point:", snapc.AccessPoint, "writable:", snapc.IsWritable, snapc.FsUid, ")")
}

type SnapshotUpdateRequest struct {
	Uid         uuid.UUID `json:"-"`
	NewName     string    `json:"new_name"`
	AccessPoint string    `json:"access_point"`
	IsWritable  bool      `json:"is_writable"`
}

func (snapu *SnapshotUpdateRequest) getApiUrl() string {
	url, err := urlutil.URLJoin(snapu.getRelatedObject().GetBasePath(), snapu.Uid.String())
	if err != nil {
		return ""
	}
	return url
}

func (snapu *SnapshotUpdateRequest) getRequiredFields() []string {
	return []string{"Uid"}
}

func (snapu *SnapshotUpdateRequest) getRelatedObject() ApiObject {
	return &Snapshot{}
}

func (snapu *SnapshotUpdateRequest) hasRequiredFields() bool {
	return ObjectRequestHasRequiredFields(snapu)
}

func (snapu *SnapshotUpdateRequest) String() string {
	return fmt.Sprintln("Snapshot(fsUid:", snapu.Uid, "writable:", snapu.IsWritable, ")")
}

type SnapshotDeleteRequest struct {
	Uid uuid.UUID `json:"-"`
}

func (snapd *SnapshotDeleteRequest) String() string {
	return fmt.Sprintln("SnapshotDeleteRequest(fsUid:", snapd.Uid, ")")
}

func (snapd *SnapshotDeleteRequest) getApiUrl() string {
	url, err := urlutil.URLJoin(snapd.getRelatedObject().GetBasePath(), snapd.Uid.String())
	if err != nil {
		return ""
	}
	return url
}

func (snapd *SnapshotDeleteRequest) getRequiredFields() []string {
	return []string{"Uid"}
}

func (snapd *SnapshotDeleteRequest) hasRequiredFields() bool {
	return ObjectRequestHasRequiredFields(snapd)
}

func (snapd *SnapshotDeleteRequest) getRelatedObject() ApiObject {
	return &Snapshot{}
}
