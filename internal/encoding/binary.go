package encoding

import (
	"encoding/binary"
	"io"
	"math"
)

func WriteUint32(w io.Writer, v uint32) error {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, v)
	_, err := w.Write(buf)
	return err
}
func ReadUint32(r io.Reader) (uint32, error) {
	buf := make([]byte, 4)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(buf), nil
}
func WriteInt64(w io.Writer, v int64) error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(v))
	_, err := w.Write(buf)
	return err
}
func ReadInt64(r io.Reader) (int64, error) {
	buf := make([]byte, 8)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(buf)), nil
}
func WriteString(w io.Writer, s string) error {
	if err := WriteUint32(w, uint32(len(s))); err != nil {
		return err
	}
	_, err := w.Write([]byte(s))
	return err
}
func ReadString(r io.Reader) (string, error) {
	lenBytes := make([]byte, 4)
	_, err := io.ReadFull(r, lenBytes)
	if err != nil {
		return "", err
	}
	nameLength := binary.LittleEndian.Uint32(lenBytes)

	nameBytes := make([]byte, nameLength)
	_, err = io.ReadFull(r, nameBytes)
	if err != nil {
		return "", err
	}

	return string(nameBytes), nil
}
func WriteFloat64(w io.Writer, v float64) error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, math.Float64bits(v))
	_, err := w.Write(buf)
	return err
}

func ReadFloat64(r io.Reader) (float64, error) {
	buf := make([]byte, 8)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}
	bits := binary.LittleEndian.Uint64(buf)
	return math.Float64frombits(bits), nil
}
