package engineio

import (
	"bytes"
	"encoding/base64"
	"errors"
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

func decodePayload(body []byte) ([]packet, error) {
	chunks := bytes.Split(body, []byte{separator})
	packets := make([]packet, 0, len(chunks))
	for _, chunk := range chunks {
		p, err := decodePacket(chunk)
		if err != nil {
			return nil, err
		}
		packets = append(packets, p)
	}
	return packets, nil
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
