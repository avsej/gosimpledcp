package simpledcp

import (
	"fmt"
	"github.com/couchbase/gocbcore"
	"time"
)

type DcpEventType uint

const (
	MutationItemType   = DcpEventType(0)
	DeletionItemType   = DcpEventType(1)
	ExpirationItemType = DcpEventType(2)
)

type DcpMutation struct {
	SeqNo    gocbcore.SeqNo
	RevNo    uint64
	Key      []byte
	Value    []byte
	Cas      gocbcore.Cas
	Flags    uint32
	Datatype uint8
	Expiry   uint32
	LockTime uint32
}

type DcpDeletion struct {
	SeqNo gocbcore.SeqNo
	RevNo uint64
	Key []byte
	Cas gocbcore.Cas
}

type DcpExpiration struct {
	SeqNo gocbcore.SeqNo
	RevNo uint64
	Key []byte
	Cas gocbcore.Cas
}

type DcpVbucketState struct {
	Entries []gocbcore.FailoverEntry
}

func (s DcpVbucketState) GetSeqNoVbUuid(seqNo gocbcore.SeqNo) gocbcore.VbUuid {
	for _, i := range s.Entries {
		if i.SeqNo <= seqNo {
			return i.VbUuid
		}
	}

	return gocbcore.VbUuid(0)
}

type DcpSnapshotState struct {
	StartSeqNo gocbcore.SeqNo
	EndSeqNo   gocbcore.SeqNo
}

type DcpStreamState struct {
	SeqNo     gocbcore.SeqNo
	SnapState DcpSnapshotState
	VbState   DcpVbucketState
}

func (s DcpStreamState) GetCurrentVbUuid() gocbcore.VbUuid {
	return s.VbState.GetSeqNoVbUuid(s.SeqNo)
}

type DcpStreamer struct {
	agent    *gocbcore.Agent
	dcpAgent *gocbcore.Agent
}

// TODO: This probably should not take an agent?
func NewDcpStreamer(config *gocbcore.AgentConfig, streamName string) (*DcpStreamer, error) {
	agent, err := gocbcore.CreateAgent(config)
	if err != nil {
		return nil, err
	}

	dcpAgent, err := gocbcore.CreateDcpAgent(config, streamName)
	if err != nil {
		agent.Close()
		return nil, err
	}

	return &DcpStreamer{
		agent:    agent,
		dcpAgent: dcpAgent,
	}, nil
}

type DcpEventHandler interface {
	Rollback(vbId uint16, newState DcpStreamState)
	NewStateEntries(vbId uint16, vbState DcpVbucketState)
	BeginSnapshot(vbId uint16, snapState DcpSnapshotState)
	Mutation(vbId uint16, item DcpMutation)
	Deletion(vbId uint16, item DcpDeletion)
	Expiration(vbId uint16, item DcpExpiration)
}

type DcpStream struct {
	vbId uint16
	parent  *DcpStreamer
	handler DcpEventHandler
	state DcpStreamState
}

func (s *DcpStream) GetState() DcpStreamState {
	return s.state
}

func (s *DcpStream) Close() {
	waitCh := make(chan bool)

	s.parent.dcpAgent.CloseStream(s.vbId, func(err error) {
		waitCh <- true
	})

	<-waitCh
}

func (s *DcpStream) performRollback(entries []gocbcore.FailoverEntry) {
	// TODO: Implement DcpStream rollback...
	panic("Not Yet Implemented")

	commonSeqNo := gocbcore.SeqNo(0)

	// Probably should roll back to snap start sequence number, all depends
	//  on whether we want to implicitly have 'at least once' semantics.

	s.state.SeqNo = commonSeqNo
	s.state.SnapState.StartSeqNo = commonSeqNo
	s.state.SnapState.EndSeqNo = commonSeqNo

	s.state.VbState.Entries = entries
	s.handler.Rollback(s.vbId, s.state)
	s.resumeStream()
}

func (s *DcpStream) resumeStream() {
	fmt.Printf("Resuming stream %d\n", s.vbId)

	s.parent.dcpAgent.OpenStream(
		s.vbId,
		s.state.GetCurrentVbUuid(),
		s.state.SeqNo,
		gocbcore.SeqNo(0xffffffffffffffff),
		s.state.SnapState.StartSeqNo,
		s.state.SnapState.EndSeqNo,
		&dcpStreamHandler{
			parent: s,
		},
		func(entries []gocbcore.FailoverEntry, err error) {
			if err == gocbcore.ErrRollback {
				s.performRollback(entries)
				return
			} else if err != nil {
				// TODO: Probably should do something better here...
				panic(fmt.Sprintf("ERR %d :: %s", s.vbId, err.Error()))
				go func() {
					time.Sleep(5 * time.Second)
					s.resumeStream()
				}()
				return
			}

			s.state.VbState.Entries = entries
			s.handler.NewStateEntries(s.vbId, s.state.VbState)

			fmt.Printf("Opened Stream %d\n", s.vbId)
		})
}

type dcpStreamHandler struct {
	parent *DcpStream
}

func (h dcpStreamHandler) SnapshotMarker(startSeqNo, endSeqNo uint64, vbId uint16, snapshotType gocbcore.SnapshotState) {
	fmt.Printf("Event SnapshotMarker %d, %d to %d\n", vbId, startSeqNo, endSeqNo)

	snapState := &h.parent.state.SnapState
	snapState.StartSeqNo = gocbcore.SeqNo(startSeqNo)
	snapState.EndSeqNo = gocbcore.SeqNo(endSeqNo)

	h.parent.handler.BeginSnapshot(vbId, DcpSnapshotState{
		StartSeqNo: gocbcore.SeqNo(startSeqNo),
		EndSeqNo:   gocbcore.SeqNo(endSeqNo),
	})
}

func (h dcpStreamHandler) Mutation(seqNo, revNo uint64, flags, expiry, lockTime uint32, cas uint64, datatype uint8, vbId uint16, key, value []byte) {
	fmt.Printf("Event Mutation %d %s\n", vbId, key)

	item := DcpMutation{}
	item.SeqNo = gocbcore.SeqNo(seqNo)
	item.RevNo = revNo
	item.Key = key
	item.Value = value
	item.Cas = gocbcore.Cas(cas)
	item.Flags = flags
	item.Expiry = expiry
	item.LockTime = lockTime
	item.Datatype = datatype
	h.parent.handler.Mutation(vbId, item)
}

func (h dcpStreamHandler) Deletion(seqNo, revNo, cas uint64, vbId uint16, key []byte) {
	fmt.Printf("Event Deletion %d %s\n", vbId, key)

	item := DcpDeletion{}
	item.SeqNo = gocbcore.SeqNo(seqNo)
	item.RevNo = revNo
	item.Key = key
	item.Cas = gocbcore.Cas(cas)
	h.parent.handler.Deletion(vbId, item)
}

func (h dcpStreamHandler) Expiration(seqNo, revNo, cas uint64, vbId uint16, key []byte) {
	fmt.Printf("Event Expiry %d %s\n", vbId, key)

	item := DcpExpiration{}
	item.SeqNo = gocbcore.SeqNo(seqNo)
	item.RevNo = revNo
	item.Key = key
	item.Cas = gocbcore.Cas(cas)
	h.parent.handler.Expiration(vbId, item)
}

func (h dcpStreamHandler) End(vbId uint16, err error) {
	fmt.Printf("Event End %d %s\n", vbId, err)

	h.parent.resumeStream()
}

func (s *DcpStreamer) OpenStream(vbId uint16, state DcpStreamState, handler DcpEventHandler) (*DcpStream, error) {
	stream := &DcpStream{
		vbId: vbId,
		parent: s,
		handler: handler,
		state: state,
	}
	stream.resumeStream()
	return stream, nil
}

func (s *DcpStreamer) GetNumVbuckets() uint16 {
	return uint16(s.dcpAgent.NumVbuckets())
}