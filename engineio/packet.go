package engineio

import (
	"bytes"
	"encoding/base64"
	"errors"
	"iter"
)

type kind byte

const (
	kindOpen kind = '0' + iota
	kindClose
	kindPing
	kindPong
	kindMessage
	kindUpgrade
	kindNoop
)

const separator = '\x1e'

var errMalformed = errors.New("engineio: malformed packet")

var kindPrefix = [...][]byte{{'0'}, {'1'}, {'2'}, {'3'}, {'4'}, {'5'}, {'6'}}

// Message is one application message carried by a session. The session keeps
// Data until it is written, so the caller must not modify it after Send.
type Message struct {
	Binary bool
	Data   []byte
}

type packet struct {
	kind   kind
	binary bool
	data   []byte
}

func (p packet) prefix() []byte {
	return kindPrefix[p.kind-kindOpen]
}

// wireSize is the number of bytes appendPayload writes for p alone, without a
// separator.
func (p packet) wireSize() int64 {
	if p.binary {
		return 1 + int64(base64.StdEncoding.EncodedLen(len(p.data)))
	}
	return 1 + int64(len(p.data))
}

func appendPayload(dst []byte, packets []packet) []byte {
	for i, p := range packets {
		if i > 0 {
			dst = append(dst, separator)
		}
		if p.binary {
			dst = append(dst, 'b')
			dst = base64.StdEncoding.AppendEncode(dst, p.data)
			continue
		}
		dst = append(dst, byte(p.kind))
		dst = append(dst, p.data...)
	}
	return dst
}

// payloadPackets yields the packets of a payload in order and decodes each one
// when it is reached, so the work and the memory do not grow with the number of
// separators. A malformed packet is yielded with its error and ends the
// sequence.
func payloadPackets(body []byte) iter.Seq2[packet, error] {
	return func(yield func(packet, error) bool) {
		for chunk := range bytes.SplitSeq(body, []byte{separator}) {
			p, err := decodePacket(chunk)
			if !yield(p, err) || err != nil {
				return
			}
		}
	}
}

func decodePacket(chunk []byte) (packet, error) {
	if len(chunk) == 0 {
		return packet{}, errMalformed
	}
	if chunk[0] == 'b' {
		data, err := base64.StdEncoding.AppendDecode(nil, chunk[1:])
		if err != nil {
			return packet{}, errMalformed
		}
		return packet{kind: kindMessage, binary: true, data: data}, nil
	}
	if chunk[0] < byte(kindOpen) || chunk[0] > byte(kindNoop) {
		return packet{}, errMalformed
	}
	return packet{kind: kind(chunk[0]), data: chunk[1:]}, nil
}
