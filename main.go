package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"

	"github.com/manifoldco/promptui"
	"github.com/samber/lo"
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
	formatProtobuf = flag.Bool("protobuf", false, "base64-encoded protobuf output format")
	listMessages   = flag.Bool("list", false, "list out available messages")
	debug          = flag.Bool("debug", false, "print debug logs")
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
	if !*debug {
		log.Default().SetOutput(io.Discard)
	}

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

	dynamic, err := interactivePopulateMessage(ctx, messageDesc)
	if err != nil {
		return err
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

func interactivePopulateMessage(ctx context.Context, desc protoreflect.MessageDescriptor) (*dynamicpb.Message, error) {
	msg := dynamicpb.NewMessage(desc)

	fields := desc.Fields()
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)

		oneOf, value, err := getValue(ctx, f)
		switch {
		case err != nil:
			return nil, err

		case value != nil:
			msg.Set(f, *value)

		case oneOf != nil:
			oneOfField := fields.ByName(oneOf.field.Name())
			msg.Set(oneOfField, oneOf.value)
		}
	}

	return msg, nil
}

func writeFormatJson(a *anypb.Any) error {
	opts := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}
	written, err := opts.Marshal(a)
	if err != nil {
		return err
	}

	_, err = fmt.Fprint(os.Stdout, string(written))
	return err
}

func writeFormatProtobuf(a *anypb.Any) error {
	written, err := proto.Marshal(a)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(os.Stdout, base64.StdEncoding.EncodeToString(written))
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

func getOneOfValue(ctx context.Context, oneOf protoreflect.OneofDescriptor) (protoreflect.FullName, protoreflect.Value, error) {
	var fields []protoreflect.FieldDescriptor

	for i := 0; i < oneOf.Fields().Len(); i++ {
		fields = append(fields, oneOf.Fields().Get(i))
	}

	p := promptui.Select{
		HideSelected: true,
		Stdout:       os.Stderr,
		Label:        "Choose oneof value",
		Items: lo.Map(fields, func(field protoreflect.FieldDescriptor, _ int) string {
			return string(field.Name())
		}),
	}
	res, err := askPrompt(ctx, p)
	if err != nil {
		return "", protoreflect.Value{}, err
	}

	field := oneOf.Fields().ByName(protoreflect.Name(res))
	if field == nil {
		panic(fmt.Sprintf("PROGRAMMER ERROR: field %s not found", res))
	}

	msg, err := interactivePopulateMessage(ctx, field.Message())
	if err != nil {
		return "", protoreflect.Value{}, err
	}
	return field.FullName(), protoreflect.ValueOfMessage(msg), nil

}

func askPrompt[P interface {
	promptui.Prompt | promptui.Select
}](ctx context.Context, prompt P) (string, error) {
	innerGet := func() (string, error) {
		p := any(prompt)
		switch p := p.(type) {
		case promptui.Prompt:
			return p.Run()

		case promptui.Select:
			_, res, err := p.Run()
			return res, err

		default:
			panic(fmt.Sprintf("unexpected prompt %T", p))
		}
	}

	errs := make(chan error)
	vals := make(chan string)
	go func() {
		val, err := innerGet()
		if err != nil {
			errs <- err
			return
		}
		vals <- val
	}()

	select {
	case <-ctx.Done():
		return "", ctx.Err()

	case err := <-errs:
		return "", err

	case v := <-vals:
		return v, nil
	}
}

// TODO: encapsulate this in some worker struct, or equivalent?
var oneOfsPopulated = map[protoreflect.FullName]struct{}{}

type oneOfRes struct {
	field protoreflect.FullName
	value protoreflect.Value
}

func getValue(ctx context.Context, field protoreflect.FieldDescriptor) (*oneOfRes, *protoreflect.Value, error) {
	log.Printf("fetching value for field %s", field.FullName())

	if oneOf := field.ContainingOneof(); oneOf != nil {
		if _, ok := oneOfsPopulated[field.FullName()]; ok {
			log.Printf("field is already set through oneof: %s", field.FullName())
			return nil, nil, nil
		}

		oneOfField, value, err := getOneOfValue(ctx, oneOf)
		if err != nil {
			return nil, nil, err
		}

		// mark all fields belonging to this oneof as done
		for i := 0; i < oneOf.Fields().Len(); i++ {
			oneOfsPopulated[oneOf.Fields().Get(i).FullName()] = struct{}{}
		}

		return &oneOfRes{oneOfField, value}, nil, nil

	}

	switch field.Kind() {

	// recursively create the inner message
	case protoreflect.MessageKind:
		msg, err := interactivePopulateMessage(ctx, field.Message())
		if err != nil {
			return nil, nil, err
		}

		v := protoreflect.ValueOfMessage(msg)
		return nil, &v, nil

	case protoreflect.StringKind:
		p := promptui.Prompt{
			Label:  field.FullName(),
			Stdout: os.Stderr,
		}
		res, err := askPrompt(ctx, p)
		if err != nil {
			return nil, nil, err
		}

		v := protoreflect.ValueOf(res)
		return nil, &v, nil

	default:
		return nil, nil, fmt.Errorf("unexpected kind: %s", field.Kind())
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
