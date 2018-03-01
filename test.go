package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
)

func main() {
	//u8 := []uint8{49, 53, 49, 57, 56, 48, 57, 49, 52, 49}
	/*u32LE := binary.LittleEndian.uint32(u8)
	fmt.Println("little-endian:", u8, "to", u32LE)
	u32BE := binary.BigEndian.uint32(u8)
	fmt.Println("big-endian:   ", u8, "to", u32BE)
	*/
	//b := []byte{0x00, 0x00, 0x03, 0xe8}
	b := []uint8{49, 53, 49, 57, 56, 48, 57, 49, 52, 49}
	b_buf := bytes.NewBuffer(b)
	var x int32
	binary.Read(b_buf, binary.BigEndian, &x)
	fmt.Println(x)

	fmt.Println(strings.Repeat("-", 100))

	x = 1519809141
	b_buf = bytes.NewBuffer([]byte{})
	binary.Write(b_buf, binary.BigEndian, x)
	fmt.Println(b_buf.Bytes())
}
