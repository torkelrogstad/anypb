package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
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
	input          = flag.String("input", ".", "'buf build' input")
	from           = flag.String("from", "", "create Protobuf message from JSON input")
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

	descriptorSet, err := buildProtoSet(ctx, *input)
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

	a := new(anypb.Any)

	switch {
	case *from != "":
		log.Printf("reading input from %s", *from)

		read, err := os.ReadFile(*from)
		if err != nil {
			return fmt.Errorf("read %s: %w", *from, err)
		}

		opts := protojson.UnmarshalOptions{
			DiscardUnknown: true,
		}
		if err := opts.Unmarshal(read, a); err != nil {
			return err
		}

	default:
		dynamic, err := interactivePopulateMessage(ctx, messageDesc)
		if err != nil {
			return err
		}
		a, err = anypb.New(dynamic)
		if err != nil {
			return err
		}
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

		if f.Kind() == protoreflect.MessageKind {
			populate, err := askBool(ctx, fmt.Sprintf("Populate message %q? (optional)", string(f.FullName())))
			if err != nil {
				return nil, err
			}
			if !populate {
				log.Printf("skipping %s", f.FullName())
				continue
			}
		}

		oneOf, value, err := getValue(ctx, f)
		switch {
		case err != nil:
			return nil, err

		case value != nil:
			log.Printf("assigning %s to %s", value, f)
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

func askBool(ctx context.Context, field string) (bool, error) {
	p := promptui.Select{
		Label:  field,
		Stdout: os.Stderr,
		Items:  []string{"true", "false"},
	}

	res, err := askPrompt(ctx, p)
	if err != nil {
		return false, err
	}

	parsed, err := strconv.ParseBool(res)
	if err != nil {
		return false, err
	}

	return parsed, nil
}

func floatPrompt(field string) promptui.Prompt {
	return promptui.Prompt{
		Label:  field,
		Stdout: os.Stderr,
		Validate: func(s string) error {
			_, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return fmt.Errorf("%q is not a valid float", s)
			}
			return nil
		},
	}
}

func intPrompt(field string) promptui.Prompt {
	return promptui.Prompt{
		Label:  field,
		Stdout: os.Stderr,
		Validate: func(s string) error {
			_, err := strconv.Atoi(s)
			if err != nil {
				return fmt.Errorf("%q is not a valid int", s)
			}
			return nil
		},
	}
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

	case protoreflect.BoolKind:
		parsed, err := askBool(ctx, string(field.FullName()))
		if err != nil {
			return nil, nil, err
		}

		v := protoreflect.ValueOfBool(parsed)
		return nil, &v, nil

	case protoreflect.Uint32Kind:
		res, err := askPrompt(ctx, intPrompt(string(field.FullName())))
		if err != nil {
			return nil, nil, err
		}

		parsed, err := strconv.Atoi(res)
		if err != nil {
			return nil, nil, err
		}

		v := protoreflect.ValueOfUint32(uint32(parsed))
		return nil, &v, nil

	case protoreflect.Int32Kind:
		res, err := askPrompt(ctx, intPrompt(string(field.FullName())))
		if err != nil {
			return nil, nil, err
		}

		parsed, err := strconv.Atoi(res)
		if err != nil {
			return nil, nil, err
		}

		v := protoreflect.ValueOfInt32(int32(parsed))
		return nil, &v, nil

	case protoreflect.Int64Kind:
		res, err := askPrompt(ctx, intPrompt(string(field.FullName())))
		if err != nil {
			return nil, nil, err
		}

		parsed, err := strconv.Atoi(res)
		if err != nil {
			return nil, nil, err
		}

		v := protoreflect.ValueOfInt64(int64(parsed))
		return nil, &v, nil

	case protoreflect.FloatKind:
		res, err := askPrompt(ctx, floatPrompt(string(field.FullName())))
		if err != nil {
			return nil, nil, err
		}

		parsed, err := strconv.ParseFloat(res, 32)
		if err != nil {
			return nil, nil, err
		}

		v := protoreflect.ValueOfFloat32(float32(parsed))
		return nil, &v, nil

	case protoreflect.DoubleKind:
		res, err := askPrompt(ctx, floatPrompt(string(field.FullName())))
		if err != nil {
			return nil, nil, err
		}

		parsed, err := strconv.ParseFloat(res, 64)
		if err != nil {
			return nil, nil, err
		}

		v := protoreflect.ValueOfFloat64(parsed)
		return nil, &v, nil

	case protoreflect.EnumKind:
		values := field.Enum().Values()
		var allEnumValues []string
		for i := 0; i < values.Len(); i++ {
			allEnumValues = append(allEnumValues, string(values.Get(i).Name()))
		}
		p := promptui.Select{
			Label:  field.FullName(),
			Stdout: os.Stderr,
			Items:  allEnumValues,
		}

		res, err := askPrompt(ctx, p)
		if err != nil {
			return nil, nil, err
		}
		enumValue := values.ByName(protoreflect.Name(res))
		if enumValue == nil {
			return nil, nil, fmt.Errorf("unknown enum: %s", res)
		}

		v := protoreflect.ValueOfEnum(enumValue.Number())
		return nil, &v, nil

	case protoreflect.BytesKind:
		p := promptui.Prompt{
			Label:  fmt.Sprintf("%s (hex)", field.FullName()),
			Stdout: os.Stderr,
		}

		res, err := askPrompt(ctx, p)
		if err != nil {
			return nil, nil, err
		}

		decoded, err := hex.DecodeString(res)
		if err != nil {
			return nil, nil, err
		}

		v := protoreflect.ValueOfBytes(decoded)
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

func buildProtoSet(ctx context.Context, input string) (*descriptorpb.FileDescriptorSet, error) {
	var out, errOut bytes.Buffer
	cmd := exec.CommandContext(ctx, "buf", "build", "-o", "-", input)
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
