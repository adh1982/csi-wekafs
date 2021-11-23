package wekafs
//TODO: make it persistent and thread-safe

import (
	"errors"
	"github.com/golang/glog"
	"sync"
)

type SnapshotDB struct {
	sync.Mutex
	snapshots *[]Snapshot
}

func NewSnapshotDB() *SnapshotDB{
	ret := &SnapshotDB{
		snapshots: &[]Snapshot{},
	}
	err := ret.Load()
	if err != nil {
		Die("Failed to load snapshot database!")
	}
	return ret
}

func (sdb *SnapshotDB) Load() error {
	return nil
}

func (sdb *SnapshotDB) Save() error {
	return nil
}

func (sdb *SnapshotDB) GetSnapRecordByName(name string) Snapshot {
	for _, snap := range *sdb.snapshots {
		if name == snap.GetName() {
			return snap
		}
	}
	glog.V(4).Infoln("Could not find existing snapshot named", name)
	return nil
}

func (sdb *SnapshotDB) GetSnapRecordById(snapId string) Snapshot {
	for _, snap := range *sdb.snapshots {
		if snapId == snap.GetId() {
			glog.V(4).Infoln("Existing snapshot was found in DB:", snapId, snap.GetName())
			return snap
		}
	}
	glog.V(4).Infoln("Could not find existing snapshot in DB having ID", snapId)
	return nil
}

func (sdb *SnapshotDB) InsertSnapshotRecord(snapshot Snapshot) error {
	for _, snap := range *sdb.snapshots {
		if snap == snapshot {
			// same object
			return nil
		}
		if snap.GetName() == snapshot.GetName() {
			if snap.getSourceVolumeId() != snapshot.getSourceVolumeId() {
				return errors.New("Same snapshot name with different source volume")
			}
		}
	}
	sdb.Lock()
	*sdb.snapshots = append(*sdb.snapshots, snapshot)
	sdb.Unlock()
	return nil
}

func (sdb *SnapshotDB) DeleteSnapshotRecord(snapshot Snapshot) error {
	for i, snap := range *sdb.snapshots {
		if snap == snapshot {
			glog.V(4).Infoln("Removing snapshot from DB since this is exactly same snapshot")
			sdb.Lock()
			*sdb.snapshots = append((*sdb.snapshots)[:i], (*sdb.snapshots)[i+1:]...)
			sdb.Unlock()
			return nil
		}
		if snap.GetName() == snapshot.GetName() && snap.getSourceVolumeId() == snapshot.getSourceVolumeId() {
			glog.V(4).Infoln("Removing snapshot from DB since this is same name and sourceVolumeId")
			sdb.Lock()
			*sdb.snapshots = append((*sdb.snapshots)[:i], (*sdb.snapshots)[i+1:]...)
			sdb.Unlock()
			return nil
		}
	}
	return errors.New("Snapshot was not found in database")
}

func (sdb *SnapshotDB) GetCount() int32 {
	return int32(len(*sdb.snapshots))
}

func (sdb *SnapshotDB) GetEntries() []Snapshot {
	return *sdb.snapshots
}