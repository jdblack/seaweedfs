package ec

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"google.golang.org/grpc"
)

// Fork-local addition (not present in upstream SeaweedFS). The cluster EC ratio
// policy lives in the filer as one JSON document, written by the admin dashboard
// or by `ec.config` and read back by whichever process is about to encode. The
// enterprise edition keeps the same document (plus a .pb mirror) and has each
// caller own its filer I/O; this fork shares one helper, which is why the shell
// and the admin dashboard cannot drift on the path or the schema.
//
// The volume server deliberately does *not* read this: the ratio reaches it on
// the generate request (VolumeEcShardsGenerateRequest.ec_shard_config).

// ecConfigDirAndName splits the policy's filer path into the directory and file
// name the filer client works with.
func ecConfigDirAndName() (dir string, name string) {
	dir, name = path.Split(erasure_coding.ECConfigPath)
	return strings.TrimSuffix(dir, "/"), name
}

// LoadECConfigFromFiler installs the policy stored at erasure_coding.ECConfigPath
// into this process. A missing document clears the policy rather than failing:
// "no policy" is the state of a fresh cluster and of an operator who deleted the
// file, and either way callers fall back to the build default. A malformed or
// invalid document is an error — encoding under a layout nobody chose is worse
// than refusing.
func LoadECConfigFromFiler(grpcDialOption grpc.DialOption, filerAddress pb.ServerAddress) error {
	dir, name := ecConfigDirAndName()
	return pb.WithGrpcFilerClient(false, 0, filerAddress, grpcDialOption,
		func(client filer_pb.SeaweedFilerClient) error {
			content, err := filer.ReadInsideFiler(context.Background(), client, dir, name)
			if err != nil {
				if errors.Is(err, filer_pb.ErrNotFound) {
					erasure_coding.ResetECConfig()
					glog.V(1).Infof("no EC config at %s%s/%s: using the build default ratio", filerAddress, dir, name)
					return nil
				}
				return fmt.Errorf("read EC config from filer %s: %w", filerAddress, err)
			}
			if err := erasure_coding.ApplyECConfigJSON(content); err != nil {
				return fmt.Errorf("load EC config from filer %s%s/%s: %w", filerAddress, dir, name, err)
			}
			glog.V(1).Infof("loaded EC config from %s%s/%s", filerAddress, dir, name)
			return nil
		})
}

// SaveECConfigToFiler writes this process's policy to the filer, creating the
// directory if the filer does not have it yet (a fresh filer predates the system
// directories, and the same path holds filer.conf).
func SaveECConfigToFiler(grpcDialOption grpc.DialOption, filerAddress pb.ServerAddress) error {
	content, err := erasure_coding.MarshalECConfigJSON()
	if err != nil {
		return err
	}
	dir, name := ecConfigDirAndName()
	return pb.WithGrpcFilerClient(false, 0, filerAddress, grpcDialOption,
		func(client filer_pb.SeaweedFilerClient) error {
			if err := ensureFilerDirectory(context.Background(), client, dir); err != nil {
				return err
			}
			if err := filer.SaveInsideFiler(context.Background(), client, dir, name, content); err != nil {
				return fmt.Errorf("save EC config to filer %s%s/%s: %w", filerAddress, dir, name, err)
			}
			glog.V(0).Infof("saved EC config to %s%s/%s", filerAddress, dir, name)
			return nil
		})
}

// ensureFilerDirectory creates dir and any missing ancestor. Existing segments
// are left alone (two writers racing on the same path both succeed: the loser
// sees the directory it tried to create).
func ensureFilerDirectory(ctx context.Context, client filer_pb.SeaweedFilerClient, dir string) error {
	parent := "/"
	for _, segment := range strings.Split(strings.Trim(dir, "/"), "/") {
		if segment == "" {
			continue
		}
		_, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: parent,
			Name:      segment,
		})
		switch {
		case err == nil:
			parent = path.Join(parent, segment)
			continue
		case !errors.Is(err, filer_pb.ErrNotFound):
			return fmt.Errorf("look up filer directory %s/%s: %w", parent, segment, err)
		}

		now := time.Now().Unix()
		if err := filer_pb.CreateEntry(ctx, client, &filer_pb.CreateEntryRequest{
			Directory: parent,
			Entry: &filer_pb.Entry{
				Name:        segment,
				IsDirectory: true,
				Attributes: &filer_pb.FuseAttributes{
					Mtime:    now,
					Crtime:   now,
					FileMode: uint32(0755),
				},
			},
		}); err != nil {
			return fmt.Errorf("create filer directory %s/%s: %w", parent, segment, err)
		}
		parent = path.Join(parent, segment)
	}
	return nil
}
