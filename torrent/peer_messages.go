package torrent

type PeerHandshakeMessage struct {
	pstr     string
	Reserved [8]byte
	InfoHash InfoHash // 20 bytes
	PeerID   PeerID   // 20 bytes
}

type MessageID uint8

const (
	ChokeID         MessageID = 0
	UnchokeID       MessageID = 1
	InterestedID    MessageID = 2
	NotInterestedID MessageID = 3
	HaveID          MessageID = 4
	BitfieldID      MessageID = 5
	RequestID       MessageID = 6
	PieceID         MessageID = 7
	CancelID        MessageID = 8
)

type PeerMessage interface {
	isMessage()
}
type KeepAlive struct{}

type Choke struct{}

type Unchoke struct{}
type Interested struct{}
type NotInterested struct{}

type Have struct {
	PieceIndex uint32
}

type Bitfield struct {
	Bits []byte
}

type BlockRequest struct {
	PieceIndex uint32
	Begin      uint32
	Length     uint32
}

type Request struct {
	Block BlockRequest
}

type CancelRequest struct {
	Block BlockRequest
}

type Piece struct {
	PieceIndex uint32
	Begin      uint32
	Data       []byte
}

func (KeepAlive) isMessage()     {}
func (Choke) isMessage()         {}
func (Unchoke) isMessage()       {}
func (Interested) isMessage()    {}
func (NotInterested) isMessage() {}
func (Have) isMessage()          {}
func (Bitfield) isMessage()      {}
func (Request) isMessage()       {}
func (CancelRequest) isMessage() {}
func (Piece) isMessage()         {}
