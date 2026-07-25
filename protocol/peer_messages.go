package protocol

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

type Message interface {
	isMessage()
}
type KeepAlive struct{}

type Choke struct{}

type Unchoke struct{}
type Interested struct{}
type NotInterested struct{}

/*
After a full piece is completed after Request recived,
broadcasted to all peers to show the index its completed
allows peers to update its bitmap
*/
type Have struct {
	PieceIndex uint32
}

/*
on connect, server peer will send bitmap if its completed pieces
*/
type Bitfield struct {
	Bits []byte
}

type BlockRequest struct {
	PieceIndex uint32
	Begin      uint32
	Length     uint32
}

/*
client peer sends to server requesting a block
*/

// request a block
type Request struct {
	Block BlockRequest
}

type CancelRequest struct {
	Block BlockRequest
}

/*
	server peer sends to requested client
	for block offset + index and associated data
*/

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
