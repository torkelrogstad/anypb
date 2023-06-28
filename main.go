package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"

	"github.com/manifoldco/promptui"
	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/anypb"
)

var (
	formatJson     = flag.Bool("json", false, "JSON output format (default)")
	formatProtobuf = flag.Bool("protobuf", false, "protobuf output format")
	listMessages   = flag.Bool("list", false, "list out available messages")
)

func main() {
	if err := realMain(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func usage(msg string) error {
	flag.Usage()
	return errors.New(msg)
}

func listAllMessages(files *protoregistry.Files) error {
	var names []protoreflect.FullName

	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		msgs := fd.Messages()
		for i := 0; i < msgs.Len(); i++ {
			names = append(names, msgs.Get(i).FullName())
		}

		return true
	})

	slices.Sort(names)
	for _, name := range names {

		// Skip uninteresting stuff
		if strings.HasPrefix(string(name), "google.protobuf") {
			continue

		}
		if _, err := fmt.Fprintln(os.Stdout, name); err != nil {
			return err
		}
	}
	return nil
}

func realMain() error {
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	fullyQualified := flag.Arg(0)
	if fullyQualified == "" && !*listMessages {
		return usage("needs exactly one argument")
	}

	descriptorSet, err := buildProtoSet(ctx)
	if err != nil {
		return err
	}

	files, err := protodesc.NewFiles(descriptorSet)
	if err != nil {
		return err
	}

	if *listMessages {
		return listAllMessages(files)
	}

	desc, err := files.FindDescriptorByName(protoreflect.FullName(fullyQualified))
	if err != nil {
		return err
	}

	messageDesc, ok := desc.(protoreflect.MessageDescriptor)
	if !ok {
		return fmt.Errorf("%s is not a message: %T", fullyQualified, desc)
	}

	if err := protoregistry.GlobalTypes.RegisterMessage(messageType{messageDesc}); err != nil {
		return err
	}

	dynamic := dynamicpb.NewMessage(messageDesc)

	fields := messageDesc.Fields()
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		value, err := getValue(ctx, f.Kind(), string(f.Name()))
		if err != nil {
			return err
		}
		dynamic.Set(f, value)
	}

	a, err := anypb.New(dynamic)
	if err != nil {
		return err
	}

	switch {
	// Default to JSON if no format is specified
	case *formatJson || (!*formatProtobuf && !*formatJson):
		return writeFormatJson(a)

	case *formatProtobuf:
		return writeFormatProtobuf(a)

	default:
		panic("programmer error: no format")
	}
}

func writeFormatJson(a *anypb.Any) error {
	opts := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}
	written, err := opts.Marshal(a)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(os.Stdout, string(written))
	return err
}

func writeFormatProtobuf(a *anypb.Any) error {
	written, err := proto.Marshal(a)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(os.Stdout, written)
	return err
}

type messageType struct {
	desc protoreflect.MessageDescriptor
}

func (m messageType) New() protoreflect.Message {
	return dynamicpb.NewMessage(m.desc)
}

func (m messageType) Zero() protoreflect.Message {
	return dynamicpb.NewMessage(m.desc)
}

func (m messageType) Descriptor() protoreflect.MessageDescriptor {
	return m.desc
}

var _ protoreflect.MessageType = new(messageType)

func getValue(ctx context.Context, kind protoreflect.Kind, field string) (protoreflect.Value, error) {
	value := make(chan protoreflect.Value)
	errs := make(chan error)

	innerGetValue := func(kind protoreflect.Kind, field string) (protoreflect.Value, error) {
		switch kind {
		case protoreflect.StringKind:
			p := promptui.Prompt{
				Label:  field,
				Stdout: os.Stderr,
			}
			res, err := p.Run()
			if err != nil {
				return protoreflect.Value{}, err
			}

			return protoreflect.ValueOf(res), nil

		default:
			return protoreflect.Value{}, fmt.Errorf("unexpected kind: %s", kind)
		}
	}

	go func() {
		v, err := innerGetValue(kind, field)
		if err != nil {
			errs <- err
			return
		}
		value <- v
	}()

	select {
	case <-ctx.Done():
		return protoreflect.Value{}, ctx.Err()

	case err := <-errs:
		return protoreflect.Value{}, err

	case v := <-value:
		return v, nil
	}
}

func buildProtoSet(ctx context.Context) (*descriptorpb.FileDescriptorSet, error) {
	var out, errOut bytes.Buffer
	cmd := exec.CommandContext(ctx, "buf", "build", "-o", "-")
	cmd.Stdout = &out
	cmd.Stderr = &errOut

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("build file descriptor set: %s", errOut.String())
	}

	var fs descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(out.Bytes(), &fs); err != nil {
		return nil, err
	}

	return &fs, nil
}
