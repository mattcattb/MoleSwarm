package main

import (
	"fmt"
	"os"

	"github.com/mattcattb/go-torrent/protocol"
)

func main() {

	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error: ", err)
		os.Exit(1)
	}
}

type CommandType string

const (
	Inspect CommandType = "inspect"
	Torrent CommandType = "torrent"
)

func run(args []string) error {

	if len(args) == 0 {
		return fmt.Errorf(
			"usage: torrent <inspect|download> [options]",
		)
	}

	switch args[0] {
	case "inspect":
		// uhhh start inspecting?
		if len(args) < 2 {
			return fmt.Errorf("invalid arg len for inspect")
		}
		filePath := args[1]
		file, err := os.Open(filePath)

		if err != nil {
			return fmt.Errorf("error opening file: %q", err)
		}

		defer file.Close()

		meta, err := protocol.ReadMetaInfo(file)

		if err != nil {
			return fmt.Errorf("error reading meta info: %q", err)
		}

		fmt.Printf("Name:         %s\n", meta.Info.Name)
		fmt.Printf("Length:       %d bytes\n", meta.Info.Length)
		fmt.Printf("Piece length: %d bytes\n", meta.Info.PieceLength)
		fmt.Printf("Pieces:       %d\n", len(meta.Info.PieceHashes))
		fmt.Printf("Info hash:    %x\n", meta.InfoHash)
		fmt.Printf("Tracker:      %s\n", meta.Announce)

		return nil

	case "download":
		return fmt.Errorf("download not implemented yet")
	default:
		return fmt.Errorf("unknown command")
	}

}
