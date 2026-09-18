package sse

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strings"
)

var ErrDone = errors.New("sse done")

type Event struct {
	Raw  []byte
	Type string
	Data string
}

type Parser struct {
	r *bufio.Reader
}

func NewParser(r io.Reader) *Parser { return &Parser{r: bufio.NewReaderSize(r, 64*1024)} }

func (p *Parser) Next() (Event, error) {
	var raw bytes.Buffer
	var event Event
	for {
		line, err := p.r.ReadString('\n')
		if len(line) > 0 {
			raw.WriteString(line)
			trimmed := strings.TrimRight(line, "\r\n")
			switch {
			case trimmed == "":
				event.Raw = append([]byte(nil), raw.Bytes()...)
				if event.Type == "" && event.Data == "" {
					raw.Reset()
					if err != nil {
						return Event{}, err
					}
					continue
				}
				if strings.TrimSpace(event.Data) == "[DONE]" {
					return event, ErrDone
				}
				return event, nil
			case strings.HasPrefix(trimmed, "event:"):
				event.Type = strings.TrimSpace(strings.TrimPrefix(trimmed, "event:"))
			case strings.HasPrefix(trimmed, "data:"):
				value := strings.TrimPrefix(trimmed, "data:")
				if strings.HasPrefix(value, " ") {
					value = value[1:]
				}
				if event.Data != "" {
					event.Data += "\n"
				}
				event.Data += value
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) && raw.Len() == 0 {
				return Event{}, io.EOF
			}
			return Event{}, err
		}
	}
}
