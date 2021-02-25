package kvraft

import (
	"bytes"

	"6.824/src/labgob"
)

func (kv *KVServer) needSnapShot() bool {
	proportion := 10

	if kv.maxraftstate > 0 && kv.maxraftstate-kv.persister.RaftStateSize() < kv.maxraftstate/proportion {
		return true
	} else {
		return false
	}
}

func (kv *KVServer) createSnapShot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.db)
	e.Encode(kv.applied)
	DPrintf("server %d is creating and sending snapshot!!!!!!!", kv.me)
	go kv.rf.Snapshot(index, w.Bytes())
}

func (kv *KVServer) LoadSnapShot(snapshot []byte) {
	DPrintf("server %d is loading snapshot", kv.me)
	if snapshot == nil || len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var db map[string]string
	var applied map[int64]string
	if d.Decode(&db) != nil || d.Decode(&applied) != nil {
		DPrintf("labgob decode error loading snapshot in server %d", kv.me)
	} else {
		kv.db = db
		kv.applied = applied
	}
}
